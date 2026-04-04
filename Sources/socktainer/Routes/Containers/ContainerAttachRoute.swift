import ContainerAPIClient
import ContainerResource
import Foundation
import NIOCore
import NIOHTTP1
import Vapor

private struct ContainerAttachQuery: Content {
    let logs: Bool?
    let stream: Bool?
    let stdin: Bool?
    let stdout: Bool?
    let stderr: Bool?
    let detachKeys: String?
}

struct ContainerAttachRoute: RouteCollection {
    let client: ClientContainerProtocol

    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.POST, pattern: "/containers/{id}/attach", use: ContainerAttachRoute.handler(client: client))
    }
}

extension ContainerAttachRoute {
    static func handler(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> Response {
        { req in
            // Each inner path handles its own upgrade mechanism:
            // - non-stdin: always HTTP streaming (200 OK)
            // - stdin with upgrade: Response.dockerTCPUpgrade (proper 101)
            // - stdin without upgrade: createDockerStreamingResponse (200 OK)
            return try await handleAttachRequest(req: req, client: client)
        }
    }

    private static func handleAttachRequest(req: Request, client: ClientContainerProtocol) async throws -> Response {
        guard let id = req.parameters.get("id") else {
            throw Abort(.badRequest, reason: "Missing container ID")
        }

        let query = try req.query.decode(ContainerAttachQuery.self)

        let logs = query.logs ?? false
        let stream = query.stream ?? false
        let stdin = query.stdin ?? false
        let stdout = query.stdout ?? false
        let stderr = query.stderr ?? false
        // NOTE: Not currently implemented, we use the default keys
        let _ = query.detachKeys ?? "ctrl-c,ctrl-p"

        // NOTE: We currently do not implement this mechanism
        //       as in Docker CLI
        guard stream || logs else {
            throw Abort(.badRequest, reason: "Either the stream or logs parameter must be true")
        }

        // If no stdout/stderr specified, default to both (Docker behavior)
        guard stdout || stderr || (!stdout && !stderr) else {
            throw Abort(.badRequest, reason: "At least one of stdout or stderr must be true")
        }

        guard let container = try await client.getContainer(id: id) else {
            throw Abort(.notFound, reason: "No such container: \(id)")
        }

        // hijack connection
        let isUpgrade = req.headers.contains(where: { $0.name.lowercased() == "upgrade" && $0.value.lowercased() == "tcp" })
        let hasConnectionUpgrade = req.headers.contains(where: { $0.name.lowercased() == "connection" && $0.value.lowercased().contains("upgrade") })

        let isTTY = container.configuration.initProcess.terminal

        // NOTE: When stdin is true, we will start the container before the client
        //       this might be a workaround for the time being.
        //       We are interested in having access to stdin file descriptor from the start
        if stdin {
            return try await handleAttachWithStdin(
                req: req,
                client: client,
                container: container,
                query: query,
                isUpgrade: isUpgrade,
                hasConnectionUpgrade: hasConnectionUpgrade,
                isTTY: isTTY
            )
        }

        let shouldUpgrade = isUpgrade && hasConnectionUpgrade

        // For stopped containers, bootstrap with pipes so we capture all output
        // regardless of how fast the container exits. The subsequent /start call
        // will see the container is already running and no-op.
        if container.status == .stopped {
            return try await handleAttachWithoutStdin(
                req: req,
                client: client,
                container: container,
                stdout: stdout,
                stderr: stderr,
                shouldUpgrade: shouldUpgrade,
                isTTY: isTTY
            )
        }

        // For running containers, poll logs
        guard shouldUpgrade else {
            let contentType = isTTY ? "application/vnd.docker.raw-stream" : "application/vnd.docker.multiplexed-stream"
            var headers = HTTPHeaders()
            headers.add(name: "Content-Type", value: contentType)

            let body = Response.Body { writer in
                Task.detached {
                    defer { _ = writer.write(.end) }
                    await Self.pollContainerLogs(
                        client: client, containerId: container.id,
                        stdout: stdout, stderr: stderr, isTTY: isTTY
                    ) { data, streamType in
                        var buffer = sharedAllocator.buffer(capacity: min(data.count + 8, 65536))
                        buffer.writeDockerFrame(streamType: streamType, data: data, ttyMode: isTTY)
                        _ = writer.write(.buffer(buffer))
                    }
                }
            }
            return Response(status: .ok, headers: headers, body: body)
        }

        return Response.dockerTCPUpgrade(
            execId: container.id,
            ttyEnabled: isTTY
        ) { channel, _ in
            await Self.pollContainerLogs(
                client: client, containerId: container.id,
                stdout: stdout, stderr: stderr, isTTY: isTTY
            ) { data, streamType in
                guard channel.isActive else { return }
                channel.eventLoop.execute {
                    var buffer = channel.allocator.buffer(capacity: min(data.count + 8, 65536))
                    buffer.writeDockerFrame(streamType: streamType, data: data, ttyMode: isTTY)
                    _ = channel.writeAndFlush(buffer)
                }
            }

            if channel.isActive {
                _ = channel.eventLoop.submit { channel.close(promise: nil) }
            }
        }
    }

    /// Polls container log handles and calls `emit` with data chunks.
    /// Returns when the container has stopped (after running) or is removed.
    private static func pollContainerLogs(
        client: ClientContainerProtocol,
        containerId: String,
        stdout: Bool,
        stderr: Bool,
        isTTY: Bool,
        emit: @Sendable (Data, DockerStreamFrame.StreamType) -> Void
    ) async {
        let pollInterval: UInt64 = 200_000_000  // 200ms
        var containerWasRunning = false
        var everReceivedData = false
        let shouldAttachStdout = stdout || (!stdout && !stderr)

        while true {
            // Check if container still exists
            do {
                guard let container = try await client.getContainer(id: containerId) else { break }
                if container.status == .running {
                    containerWasRunning = true
                } else if containerWasRunning || everReceivedData {
                    break  // Container finished
                }
            } catch {
                break
            }

            // Try to get log handles
            var logHandles: [FileHandle] = []
            do {
                logHandles = try await ContainerClient().logs(id: containerId)
            } catch {}

            defer {
                for handle in logHandles { try? handle.close() }
            }

            guard !logHandles.isEmpty else {
                try? await Task.sleep(nanoseconds: pollInterval)
                continue
            }

            // Read available data from handles
            var consecutiveEmptyReads = 0
            while true {
                do {
                    guard let container = try await client.getContainer(id: containerId) else { return }
                    if container.status == .running {
                        containerWasRunning = true
                    } else if containerWasRunning || everReceivedData {
                        return
                    }
                } catch {
                    return
                }

                var hasData = false

                if shouldAttachStdout, logHandles.indices.contains(0) {
                    let data = logHandles[0].availableData
                    if !data.isEmpty {
                        hasData = true
                        everReceivedData = true
                        emit(data, .stdout)
                    }
                }

                if !hasData {
                    consecutiveEmptyReads += 1
                    let delay: UInt64 = consecutiveEmptyReads >= 50 ? 500_000_000 : 50_000_000
                    if consecutiveEmptyReads >= 50 { consecutiveEmptyReads = 0 }
                    try? await Task.sleep(nanoseconds: delay)
                } else {
                    consecutiveEmptyReads = 0
                    try? await Task.sleep(nanoseconds: 5_000_000)
                }
            }
        }
    }

    private static func handleAttachWithStdin(
        req: Request,
        client: ClientContainerProtocol,
        container: ContainerSnapshot,
        query: ContainerAttachQuery,
        isUpgrade: Bool,
        hasConnectionUpgrade: Bool,
        isTTY: Bool
    ) async throws -> Response {

        let connectionHeader = req.headers.first(name: "Connection")?.lowercased()
        let upgradeHeader = req.headers.first(name: "Upgrade")?.lowercased()
        let shouldUpgrade = connectionHeader?.contains("upgrade") == true && upgradeHeader == "tcp"

        guard let currentContainer = try await client.getContainer(id: container.id) else {
            throw Abort(.notFound, reason: "Container not found")
        }

        // NOTE: For true docker run -it behavior, we need to control the main process stdio,
        //       this means we need to bootstrap the container with our own pipes
        // WARN: docker compose reaches this logic
        guard currentContainer.status == .stopped else {
            throw Abort(.conflict, reason: "Container is in \(currentContainer.status) state and cannot be attached to")
        }
        return try await createContainerForAttachment(
            req: req,
            client: client,
            container: currentContainer,
            query: query,
            shouldUpgrade: shouldUpgrade,
            isTTY: isTTY
        )
    }

    // Handle attachment to stopped containers by bootstrapping with our stdio
    private static func createContainerForAttachment(
        req: Request,
        client: ClientContainerProtocol,
        container: ContainerSnapshot,
        query: ContainerAttachQuery,
        shouldUpgrade: Bool,
        isTTY: Bool
    ) async throws -> Response {

        let attachStdout = query.stdout ?? true
        let attachStderr = query.stderr ?? !isTTY

        // Safe pipes: raw pipe(2) + dup() — no Foundation Pipe auto-close issues
        let stdinPipe: StdinPipe = makeStdinPipe()
        let stdoutPipe: OutputPipe? = attachStdout ? makeOutputPipe() : nil
        let stderrPipe: OutputPipe? = (attachStderr && !isTTY) ? makeOutputPipe() : nil

        let stdio = [
            stdinPipe.forProcess,
            stdoutPipe?.forProcess,
            stderrPipe?.forProcess,
        ]

        let process: ClientProcess
        do {
            process = try await ContainerClient().bootstrap(id: container.id, stdio: stdio)
        } catch {
            throw Abort(.internalServerError, reason: "Failed to bootstrap container: \(error.localizedDescription)")
        }

        do {
            try await process.start()
        } catch {
            throw Abort(.internalServerError, reason: "Failed to start main process: \(error.localizedDescription)")
        }

        guard shouldUpgrade else {

            return ConnectionHijackingMiddleware.createDockerStreamingResponse(
                request: req,
                ttyEnabled: isTTY
            ) { streamContinuation in

                await withTaskGroup(of: Void.self) { group in
                    // Process monitor - when process exits, close pipes and finish stream
                    group.addTask {
                        defer {
                            if let fd = stdoutPipe?.forCleanup { close(fd) }
                            if let fd = stderrPipe?.forCleanup { close(fd) }
                            close(stdinPipe.forUsFd)

                            streamContinuation.finish()
                        }

                        do {
                            let _ = try await process.wait()
                        } catch {
                        }
                    }

                    if let stdoutHandle = stdoutPipe?.forUs {
                        group.addTask {
                            defer { try? stdoutHandle.close() }
                            while true {
                                do {
                                    guard let data = try stdoutHandle.read(upToCount: 8192), !data.isEmpty else {
                                        break
                                    }
                                    let capacity = min(data.count + (isTTY ? 0 : 8), 65536)
                                    var buffer = sharedAllocator.buffer(capacity: capacity)
                                    buffer.writeDockerFrame(streamType: .stdout, data: data, ttyMode: isTTY)
                                    streamContinuation.yield(buffer)
                                } catch {
                                    break
                                }
                            }
                        }
                    }

                    if let stderrHandle = stderrPipe?.forUs {
                        group.addTask {
                            defer { try? stderrHandle.close() }
                            while true {
                                do {
                                    guard let data = try stderrHandle.read(upToCount: 8192), !data.isEmpty else {
                                        break
                                    }
                                    let capacity = min(data.count + 8, 65536)
                                    var buffer = sharedAllocator.buffer(capacity: capacity)
                                    buffer.writeDockerFrame(streamType: .stderr, data: data, ttyMode: isTTY)
                                    streamContinuation.yield(buffer)
                                } catch {
                                    break
                                }
                            }
                        }
                    }

                    let stdinFd = stdinPipe.forUsFd
                    group.addTask {
                        defer {
                            close(stdinFd)
                        }

                        do {
                            for try await var buf in req.body {
                                if let data = buf.readData(length: buf.readableBytes) {
                                    data.withUnsafeBytes { ptr in
                                        guard let base = ptr.baseAddress else { return }
                                        var remaining = ptr.count
                                        var offset = 0
                                        while remaining > 0 {
                                            let written = Darwin.write(stdinFd, base + offset, remaining)
                                            if written <= 0 { return }
                                            offset += written
                                            remaining -= written
                                        }
                                    }
                                }
                            }
                        } catch {
                        }
                    }

                    for await _ in group {}
                }
            }
        }

        return Response.dockerTCPUpgrade(
            execId: container.id,
            ttyEnabled: isTTY
        ) { channel, tcpHandler in

            tcpHandler.setStdinFd(stdinPipe.forUsFd)

            await withTaskGroup(of: Void.self) { group in
                if let stdoutHandle = stdoutPipe?.forUs {
                    group.addTask {
                        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                            let dispatchIO = DispatchIO(
                                type: .stream,
                                fileDescriptor: stdoutHandle.fileDescriptor,
                                queue: DispatchQueue.global(qos: .userInteractive)
                            ) { error in
                                continuation.resume()
                            }

                            dispatchIO.setLimit(lowWater: 1)
                            dispatchIO.setLimit(highWater: 8192)

                            let state = DockerConnectionState()

                            @Sendable func readNextChunk() {
                                dispatchIO.read(
                                    offset: off_t.max,
                                    length: 8192,
                                    queue: DispatchQueue.global(qos: .userInteractive)
                                ) { done, data, error in
                                    guard !done || error == 0 else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }
                                    guard let data = data else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }
                                    guard !data.isEmpty || !done else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }

                                    if !data.isEmpty {
                                        channel.eventLoop.execute {
                                            let capacity = min(data.count + (isTTY ? 0 : 8), 65536)
                                            var outputBuffer = channel.allocator.buffer(capacity: capacity)
                                            if isTTY {
                                                outputBuffer.writeBytes(data)
                                            } else {
                                                outputBuffer.writeDockerFrame(streamType: .stdout, data: Data(data), ttyMode: false)
                                            }
                                            _ = channel.writeAndFlush(outputBuffer)
                                        }
                                    }

                                    if done && !state.shouldStop() {
                                        DispatchQueue.global(qos: .userInteractive).async {
                                            readNextChunk()
                                        }
                                    }
                                }
                            }

                            readNextChunk()
                        }

                        try? stdoutHandle.close()
                    }
                }

                if let stderrHandle = stderrPipe?.forUs {
                    group.addTask {
                        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                            let dispatchIO = DispatchIO(
                                type: .stream,
                                fileDescriptor: stderrHandle.fileDescriptor,
                                queue: DispatchQueue.global(qos: .userInteractive)
                            ) { error in
                                continuation.resume()
                            }

                            dispatchIO.setLimit(lowWater: 1)
                            dispatchIO.setLimit(highWater: 8192)

                            let state = DockerConnectionState()

                            @Sendable func readNextChunk() {
                                dispatchIO.read(
                                    offset: off_t.max,
                                    length: 8192,
                                    queue: DispatchQueue.global(qos: .userInteractive)
                                ) { done, data, error in
                                    guard !done || error == 0 else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }
                                    guard let data = data else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }
                                    guard !data.isEmpty || !done else {
                                        state.finish {
                                            dispatchIO.close()
                                        }
                                        return
                                    }

                                    if !data.isEmpty {
                                        channel.eventLoop.execute {
                                            let capacity = min(data.count + (isTTY ? 0 : 8), 65536)
                                            var outputBuffer = channel.allocator.buffer(capacity: capacity)
                                            if isTTY {
                                                outputBuffer.writeBytes(data)
                                            } else {
                                                outputBuffer.writeDockerFrame(streamType: .stderr, data: Data(data), ttyMode: false)
                                            }
                                            _ = channel.writeAndFlush(outputBuffer)
                                        }
                                    }

                                    if done && !state.shouldStop() {
                                        DispatchQueue.global(qos: .userInteractive).async {
                                            readNextChunk()
                                        }
                                    }
                                }
                            }

                            readNextChunk()
                        }

                        try? stderrHandle.close()
                    }
                }

                group.addTask {
                    let maxWaits = 6000  // 10 minutes max (6000 * 100ms)
                    for _ in 0..<maxWaits {
                        guard channel.isActive else { break }
                        try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
                    }
                }

                group.addTask {
                    do {
                        let _ = try await process.wait()
                    } catch {
                    }

                    // Give a small delay for any final output to be processed
                    try? await Task.sleep(nanoseconds: 200_000_000)  // 200ms

                    // Close dup'd stdout/stderr write ends to signal EOF to DispatchIO readers
                    if let fd = stdoutPipe?.forCleanup { close(fd) }
                    if let fd = stderrPipe?.forCleanup { close(fd) }
                    tcpHandler.setStdinFd(-1)

                    // Close the channel gracefully
                    if channel.isActive {
                        _ = channel.eventLoop.submit {
                            channel.close(promise: nil)
                        }
                    }
                }

                for await _ in group {}
            }
        }
    }

    /// Attach to a stopped container by setting up pipes and waiting for /start to bootstrap.
    ///
    /// Flow: creates pipes → stores write fds in AttachManager → returns streaming response.
    /// The pipe reads block until /start bootstraps the container with our pipe write fds.
    /// /start also provides the process via AttachManager so we can monitor its lifecycle.
    private static func handleAttachWithoutStdin(
        req: Request,
        client: ClientContainerProtocol,
        container: ContainerSnapshot,
        stdout: Bool,
        stderr: Bool,
        shouldUpgrade: Bool,
        isTTY: Bool
    ) async throws -> Response {

        let attachStdout = stdout || (!stdout && !stderr)
        let attachStderr = stderr && !isTTY

        // Safe pipes: raw pipe(2) + dup()
        let stdoutPipe: OutputPipe? = attachStdout ? makeOutputPipe() : nil
        let stderrPipe: OutputPipe? = attachStderr ? makeOutputPipe() : nil

        // Extra dup of write ends for /start to pass to bootstrap
        let stdoutWriteFdForStart: Int32 = stdoutPipe.map { dup($0.forCleanup) } ?? -1
        let stderrWriteFdForStart: Int32 = stderrPipe.map { dup($0.forCleanup) } ?? -1

        // Store for /start to consume
        await AttachManager.shared.storePipes(
            id: container.id,
            attach: .init(stdoutWriteFd: stdoutWriteFdForStart, stderrWriteFd: stderrWriteFdForStart)
        )

        let containerId = container.id

        guard shouldUpgrade else {
            return ConnectionHijackingMiddleware.createDockerStreamingResponse(
                request: req,
                ttyEnabled: isTTY
            ) { streamContinuation in

                await withTaskGroup(of: Void.self) { group in
                    // Process monitor — waits for /start to provide process, then waits for exit
                    group.addTask {
                        defer {
                            if let fd = stdoutPipe?.forCleanup { close(fd) }
                            if let fd = stderrPipe?.forCleanup { close(fd) }
                            streamContinuation.finish()
                            Task { await AttachManager.shared.cleanup(id: containerId) }
                        }
                        guard let process = await AttachManager.shared.awaitProcess(id: containerId) else { return }
                        do {
                            let _ = try await process.wait()
                        } catch {}
                    }

                    if let stdoutHandle = stdoutPipe?.forUs {
                        group.addTask {
                            defer { try? stdoutHandle.close() }
                            while true {
                                do {
                                    guard let data = try stdoutHandle.read(upToCount: 8192), !data.isEmpty else { break }
                                    let capacity = min(data.count + (isTTY ? 0 : 8), 65536)
                                    var buffer = sharedAllocator.buffer(capacity: capacity)
                                    buffer.writeDockerFrame(streamType: .stdout, data: data, ttyMode: isTTY)
                                    streamContinuation.yield(buffer)
                                } catch { break }
                            }
                        }
                    }

                    if let stderrHandle = stderrPipe?.forUs {
                        group.addTask {
                            defer { try? stderrHandle.close() }
                            while true {
                                do {
                                    guard let data = try stderrHandle.read(upToCount: 8192), !data.isEmpty else { break }
                                    let capacity = min(data.count + 8, 65536)
                                    var buffer = sharedAllocator.buffer(capacity: capacity)
                                    buffer.writeDockerFrame(streamType: .stderr, data: data, ttyMode: isTTY)
                                    streamContinuation.yield(buffer)
                                } catch { break }
                            }
                        }
                    }

                    for await _ in group {}
                }
            }
        }

        return Response.dockerTCPUpgrade(
            execId: container.id,
            ttyEnabled: isTTY
        ) { channel, _ in

            await withTaskGroup(of: Void.self) { group in
                if let stdoutHandle = stdoutPipe?.forUs {
                    group.addTask {
                        let dispatchIO = DispatchIO(
                            type: .stream,
                            fileDescriptor: stdoutHandle.fileDescriptor,
                            queue: DispatchQueue.global(qos: .userInteractive)
                        ) { error in
                            try? stdoutHandle.close()
                        }

                        dispatchIO.setLimit(lowWater: 1)
                        dispatchIO.setLimit(highWater: 8192)

                        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                            var hasCompleted = false
                            let completionLock = NSLock()

                            func safeComplete() {
                                completionLock.lock()
                                defer { completionLock.unlock() }
                                guard !hasCompleted else { return }
                                hasCompleted = true
                                dispatchIO.close()
                                continuation.resume()
                            }

                            dispatchIO.read(
                                offset: 0,
                                length: Int.max,
                                queue: DispatchQueue.global(qos: .userInteractive)
                            ) { done, data, error in

                                completionLock.lock()
                                let shouldProcess = !hasCompleted && channel.isActive
                                completionLock.unlock()

                                if shouldProcess, let data = data, !data.isEmpty {
                                    channel.eventLoop.execute {
                                        let capacity = min(data.count + (isTTY ? 0 : 8), 65536)
                                        var outputBuffer = channel.allocator.buffer(capacity: capacity)
                                        if isTTY {
                                            outputBuffer.writeBytes(data)
                                        } else {
                                            outputBuffer.writeDockerFrame(streamType: .stdout, data: Data(data), ttyMode: false)
                                        }
                                        _ = channel.writeAndFlush(outputBuffer)
                                    }
                                }

                                if done || error != 0 || !channel.isActive {
                                    safeComplete()
                                }
                            }
                        }
                    }
                }

                if let stderrHandle = stderrPipe?.forUs {
                    group.addTask {
                        let dispatchIO = DispatchIO(
                            type: .stream,
                            fileDescriptor: stderrHandle.fileDescriptor,
                            queue: DispatchQueue.global(qos: .userInteractive)
                        ) { error in
                            try? stderrHandle.close()
                        }

                        dispatchIO.setLimit(lowWater: 1)
                        dispatchIO.setLimit(highWater: 8192)

                        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
                            var hasCompleted = false
                            let completionLock = NSLock()

                            func safeComplete() {
                                completionLock.lock()
                                defer { completionLock.unlock() }
                                guard !hasCompleted else { return }
                                hasCompleted = true
                                dispatchIO.close()
                                continuation.resume()
                            }

                            dispatchIO.read(
                                offset: 0,
                                length: Int.max,
                                queue: DispatchQueue.global(qos: .userInteractive)
                            ) { done, data, error in

                                completionLock.lock()
                                let shouldProcess = !hasCompleted && channel.isActive
                                completionLock.unlock()

                                if shouldProcess, let data = data, !data.isEmpty {
                                    channel.eventLoop.execute {
                                        let capacity = min(data.count + 8, 65536)
                                        var outputBuffer = channel.allocator.buffer(capacity: capacity)
                                        outputBuffer.writeDockerFrame(streamType: .stderr, data: Data(data), ttyMode: false)
                                        _ = channel.writeAndFlush(outputBuffer)
                                    }
                                }

                                if done || error != 0 || !channel.isActive {
                                    safeComplete()
                                }
                            }
                        }
                    }
                }

                // Process monitor — waits for /start to provide process, then waits for exit
                group.addTask {
                    guard let process = await AttachManager.shared.awaitProcess(id: containerId) else {
                        if let fd = stdoutPipe?.forCleanup { close(fd) }
                        if let fd = stderrPipe?.forCleanup { close(fd) }
                        if channel.isActive {
                            _ = channel.eventLoop.submit { channel.close(promise: nil) }
                        }
                        return
                    }

                    do {
                        let _ = try await process.wait()
                    } catch {}

                    try? await Task.sleep(nanoseconds: 200_000_000)  // drain remaining output

                    if let fd = stdoutPipe?.forCleanup { close(fd) }
                    if let fd = stderrPipe?.forCleanup { close(fd) }

                    if channel.isActive {
                        _ = channel.eventLoop.submit {
                            channel.close(promise: nil)
                        }
                    }

                    Task { await AttachManager.shared.cleanup(id: containerId) }
                }

                for await _ in group {}
            }
        }
    }

}
