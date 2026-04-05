import Foundation
import NIOCore
import NIOHTTP1
import Vapor

// MARK: - Safe pipe helpers
// Uses raw pipe(2) + dup() to create fully independent fd copies.
// Unlike Foundation's Pipe(), no auto-close on dealloc — all fds are explicitly managed.

/// Pipe for process stdin: SDK reads from `forProcess`, we write to `forUsFd`.
struct StdinPipe {
    let forProcess: FileHandle  // closeOnDealloc: false — SDK owns it
    let forUsFd: Int32          // raw fd — use Darwin.write(), caller closes
}

/// Pipe for process stdout/stderr: SDK writes to `forProcess`, we read from `forUs`.
/// Close `forCleanup` to signal EOF to reader after process exits.
struct OutputPipe {
    let forProcess: FileHandle  // closeOnDealloc: false — SDK owns it
    let forUs: FileHandle       // closeOnDealloc: true — our read handle
    let forCleanup: Int32       // raw fd — close() to signal EOF
}

func makeStdinPipe() -> StdinPipe {
    var fds = [Int32](repeating: -1, count: 2)
    precondition(pipe(&fds) == 0, "pipe() failed")
    let readCopy = dup(fds[0])
    let writeCopy = dup(fds[1])
    close(fds[0])
    close(fds[1])
    return StdinPipe(
        forProcess: FileHandle(fileDescriptor: readCopy, closeOnDealloc: false),
        forUsFd: writeCopy
    )
}

func makeOutputPipe() -> OutputPipe {
    var fds = [Int32](repeating: -1, count: 2)
    precondition(pipe(&fds) == 0, "pipe() failed")
    let readCopy = dup(fds[0])
    let writeCopy1 = dup(fds[1])
    let writeCopy2 = dup(fds[1])
    close(fds[0])
    close(fds[1])
    return OutputPipe(
        forProcess: FileHandle(fileDescriptor: writeCopy1, closeOnDealloc: false),
        forUs: FileHandle(fileDescriptor: readCopy, closeOnDealloc: true),
        forCleanup: writeCopy2
    )
}

/// Thread-safe state management for Docker I/O operations to prevent race conditions
public final class DockerConnectionState: @unchecked Sendable {
    private let lock = NSLock()
    private var _isFinished = false
    private var _hasResumed = false

    public init() {}

    public func shouldStop() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return _isFinished
    }

    public func finish(completion: () -> Void) {
        lock.lock()
        defer { lock.unlock() }
        if !_hasResumed {
            _isFinished = true
            _hasResumed = true
            completion()
        }
    }
}

/// Shared allocator for memory efficiency across Docker operations
public let sharedAllocator = ByteBufferAllocator()

/// This provides TCP connection hijacking for Docker exec endpoints
public struct DockerTCPUpgrader: Upgrader, Sendable {
    let execId: String
    let ttyEnabled: Bool
    let streamHandler: @Sendable (Channel, DockerTCPHandler) async throws -> Void

    public init(execId: String, ttyEnabled: Bool, streamHandler: @escaping @Sendable (Channel, DockerTCPHandler) async throws -> Void) {
        self.execId = execId
        self.ttyEnabled = ttyEnabled
        self.streamHandler = streamHandler
    }

    public func applyUpgrade(req: Request, res: Response) -> HTTPServerProtocolUpgrader {
        DockerTCPProtocolUpgrader(
            execId: execId,
            ttyEnabled: ttyEnabled,
            streamHandler: streamHandler
        )
    }
}

/// Internal protocol upgrader that handles the actual NIO channel upgrade for Docker TCP
private struct DockerTCPProtocolUpgrader: HTTPServerProtocolUpgrader {
    let execId: String
    let ttyEnabled: Bool
    let streamHandler: @Sendable (Channel, DockerTCPHandler) async throws -> Void

    var supportedProtocol: String { "tcp" }
    var requiredUpgradeHeaders: [String] { ["upgrade"] }

    func buildUpgradeResponse(
        channel: Channel,
        upgradeRequest: HTTPRequestHead,
        initialResponseHeaders: HTTPHeaders
    ) -> EventLoopFuture<HTTPHeaders> {

        var headers = HTTPHeaders()
        headers.add(name: "Connection", value: "Upgrade")
        headers.add(name: "Upgrade", value: "tcp")

        return channel.eventLoop.makeSucceededFuture(headers)
    }

    func upgrade(context: ChannelHandlerContext, upgradeRequest: HTTPRequestHead) -> EventLoopFuture<Void> {

        let tcpHandler = DockerTCPHandler(execId: execId, ttyEnabled: ttyEnabled)

        let channel = context.channel
        let eventLoop = context.eventLoop

        // Allow remote half-closure so that when Docker CLI closes write-half
        // (no stdin to send), we keep the channel open for stdout writes.
        return channel.setOption(ChannelOptions.allowRemoteHalfClosure, value: true).flatMap {
        channel.pipeline.addHandler(tcpHandler).flatMap { _ in
            _ = Task.detached { [streamHandler] in
                do {
                    try await streamHandler(channel, tcpHandler)
                } catch {
                    eventLoop.execute {
                        channel.close(promise: nil)
                    }
                }
            }

            return eventLoop.makeSucceededVoidFuture()
        }
        }
    }
}

/// Channel handler that manages raw TCP communication after HTTP upgrade.
/// Uses raw Darwin.write() instead of Foundation FileHandle to avoid ENOTSUP issues.
public final class DockerTCPHandler: ChannelInboundHandler, @unchecked Sendable {
    public typealias InboundIn = ByteBuffer

    let execId: String
    let ttyEnabled: Bool
    private let lock = NSLock()
    private var stdinFd: Int32 = -1
    private var pendingData: [Data] = []
    private static let logger = Logger(label: "DockerTCPHandler")

    init(execId: String, ttyEnabled: Bool) {
        self.execId = execId
        self.ttyEnabled = ttyEnabled
    }

    public func channelActive(context: ChannelHandlerContext) {
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let buffer = unwrapInboundIn(data)
        guard let data = buffer.getData(at: 0, length: buffer.readableBytes) else { return }

        lock.lock()
        let fd = stdinFd
        if fd >= 0 {
            lock.unlock()
            data.withUnsafeBytes { ptr in
                guard let base = ptr.baseAddress else { return }
                var remaining = ptr.count
                var offset = 0
                while remaining > 0 {
                    let written = Darwin.write(fd, base + offset, remaining)
                    if written < 0 {
                        Self.logger.error("[\(self.execId)] stdin write failed (\(data.count) bytes, fd=\(fd)): errno=\(errno)")
                        break
                    }
                    if written == 0 { break }
                    offset += written
                    remaining -= written
                }
            }
        } else {
            pendingData.append(data)
            lock.unlock()
        }
    }

    public func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if (event as? ChannelEvent) == .inputClosed {
            Self.logger.info("[\(execId)] inputClosed — closing stdinFd")
            lock.lock()
            if stdinFd >= 0 { close(stdinFd) }
            stdinFd = -1
            lock.unlock()
        } else {
            context.fireUserInboundEventTriggered(event)
        }
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        Self.logger.error("[\(execId)] errorCaught: \(error)")
        context.close(promise: nil)
    }

    public func channelInactive(context: ChannelHandlerContext) {
        lock.lock()
        if stdinFd >= 0 { close(stdinFd) }
        stdinFd = -1
        lock.unlock()
    }

    /// Set the stdin fd. Pass -1 to clear (closes the fd).
    public func setStdinFd(_ fd: Int32) {
        lock.lock()
        let oldFd = stdinFd
        stdinFd = fd
        let buffered = pendingData
        pendingData = []
        // Close old fd inside the lock to prevent TOCTOU with channelRead
        if oldFd >= 0 && oldFd != fd { close(oldFd) }
        lock.unlock()

        // Flush any data that arrived before the fd was set
        if fd >= 0 {
            for data in buffered {
                let ok = data.withUnsafeBytes { ptr -> Bool in
                    guard let base = ptr.baseAddress else { return true }
                    var remaining = ptr.count
                    var offset = 0
                    while remaining > 0 {
                        let written = Darwin.write(fd, base + offset, remaining)
                        if written <= 0 { return false }
                        offset += written
                        remaining -= written
                    }
                    return true
                }
                if !ok { break }
            }
        }
    }
}

/// Helper extension to create Docker upgrader responses
extension Response {
    /// Creates a response that will upgrade to Docker TCP protocol
    static func dockerTCPUpgrade(
        execId: String,
        ttyEnabled: Bool,
        streamHandler: @escaping @Sendable (Channel, DockerTCPHandler) async throws -> Void
    ) -> Response {
        let upgrader = DockerTCPUpgrader(
            execId: execId,
            ttyEnabled: ttyEnabled,
            streamHandler: streamHandler
        )

        let response = Response(status: .switchingProtocols)
        response.upgrader = upgrader

        return response
    }
}

/// Middleware that enables HTTP connection hijacking for Docker API compatibility
/// This allows endpoints to upgrade to raw TCP for bidirectional stdin/stdout/stderr communication
public struct ConnectionHijackingMiddleware: AsyncMiddleware {

    public func respond(to request: Request, chainingTo next: AsyncResponder) async throws -> Response {

        // Only intercept specific paths that need hijacking
        guard shouldHijackConnection(for: request) else {
            return try await next.respond(to: request)
        }

        // Check if client requested connection upgrade
        let connectionHeader = request.headers.first(name: "Connection")?.lowercased()
        let upgradeHeader = request.headers.first(name: "Upgrade")?.lowercased()

        let shouldUpgrade = connectionHeader?.contains("upgrade") == true && upgradeHeader == "tcp"

        let response = try await next.respond(to: request)

        // If client requested upgrade and handler returned streaming content
        if shouldUpgrade && response.status == .ok {

            // For hijacked connections, create minimal headers (no content-type for raw TCP)
            var hijackedHeaders: HTTPHeaders = [:]
            hijackedHeaders.add(name: "Connection", value: "Upgrade")
            hijackedHeaders.add(name: "Upgrade", value: "tcp")

            // Use the original response body but with HTTP 101 status
            // This should work because after 101, the body becomes raw TCP data
            let hijackedResponse = Response(
                status: .switchingProtocols,
                headers: hijackedHeaders,
                body: response.body
            )
            return hijackedResponse
        }

        // For non-upgrade requests, ensure proper content-type is set
        if response.status == .ok {
            var headers = response.headers

            // Determine content type based on TTY setting if not already set
            if headers.first(name: "Content-Type") == nil {
                let ttyEnabled = request.query["tty"] == "true" || request.query["Tty"] == "true"
                let contentType = ttyEnabled ? "application/vnd.docker.raw-stream" : "application/vnd.docker.multiplexed-stream"

                headers.replaceOrAdd(name: "Content-Type", value: contentType)
            }

            return Response(
                status: response.status,
                headers: headers,
                body: response.body
            )
        }

        return response
    }

    private func shouldHijackConnection(for request: Request) -> Bool {
        let path = request.url.path

        if path.contains("/attach") && !path.contains("/attach/ws") {
            return true
        }

        // Check for exec start endpoints
        if path.contains("/exec/") && path.hasSuffix("/start") {
            return true
        }

        return false
    }
}

/// Extension to support raw TCP hijacking for interactive sessions
/// Using NIO server implementation
extension ConnectionHijackingMiddleware {

    /// Creates a streaming response that handles Docker's TCP upgrade expectation
    static func createDockerStreamingResponse(
        request: Request,
        ttyEnabled: Bool,
        streamHandler: @escaping @Sendable (AsyncThrowingStream<ByteBuffer, Error>.Continuation) async throws -> Void
    ) -> Response {

        let contentType = ttyEnabled ? "application/vnd.docker.raw-stream" : "application/vnd.docker.multiplexed-stream"

        var headers: HTTPHeaders = [:]
        headers.add(name: "Content-Type", value: contentType)

        let body = Response.Body(stream: { writer in
            let (stream, continuation) = AsyncThrowingStream<ByteBuffer, Error>.makeStream()

            Task.detached {
                do {
                    // Start the stream handler
                    try await streamHandler(continuation)
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            Task.detached {
                do {
                    for try await buffer in stream {
                        _ = writer.write(.buffer(buffer))
                    }
                    _ = writer.write(.end)
                } catch {
                    _ = writer.write(.end)
                }
            }
        })

        // Always use 200 OK for HTTP streaming responses.
        // True TCP upgrades (101) use Response.dockerTCPUpgrade with a proper NIO upgrader.
        return Response(status: .ok, headers: headers, body: body)
    }
}

/// Utility for creating multiplexed stream frames
public struct DockerStreamFrame {
    public enum StreamType: UInt8, Sendable {
        case stdin = 0  // Written on stdout
        case stdout = 1
        case stderr = 2
    }

    public let streamType: StreamType
    public let data: Data

    public init(streamType: StreamType, data: Data) {
        self.streamType = streamType
        self.data = data
    }
}

/// Extension to ByteBuffer for Docker stream handling
extension ByteBuffer {
    /// Writes a Docker stream frame to the buffer
    mutating func writeDockerFrame(streamType: DockerStreamFrame.StreamType, data: Data, ttyMode: Bool) {
        if ttyMode {
            // In TTY mode, data is sent raw without framing
            writeBytes(data)
        } else {
            // In non-TTY mode, use multiplexed format with 8-byte headers
            // Create 8-byte header: [stream_type, 0, 0, 0, size_big_endian]
            writeInteger(streamType.rawValue, as: UInt8.self)  // Stream type
            writeInteger(UInt8(0), as: UInt8.self)  // Padding
            writeInteger(UInt8(0), as: UInt8.self)  // Padding
            writeInteger(UInt8(0), as: UInt8.self)  // Padding
            writeInteger(UInt32(data.count), endianness: .big, as: UInt32.self)
            writeBytes(data)
        }
    }
}
