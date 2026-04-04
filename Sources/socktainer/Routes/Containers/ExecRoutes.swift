import ContainerAPIClient
import Foundation
import NIOCore
import Vapor

// Singleton ExecManager to track exec configs
actor ExecManager {
    static let shared = ExecManager()

    struct ExecConfig {
        let containerId: String
        let cmd: [String]
        let attachStdin: Bool
        let attachStdout: Bool
        let attachStderr: Bool
        let tty: Bool
        let detach: Bool
        var running: Bool = false
        var exitCode: Int? = nil
    }

    private var storage: [String: ExecConfig] = [:]

    func create(config: ExecConfig) -> String {
        let id = UUID().uuidString
        storage[id] = config
        return id
    }

    func get(id: String) -> ExecConfig? {
        storage[id]
    }

    func markRunning(id: String) {
        storage[id]?.running = true
    }

    func markCompleted(id: String, exitCode: Int32) {
        storage[id]?.running = false
        storage[id]?.exitCode = Int(exitCode)
    }
}

// Request & Response DTOs
struct CreateExecRequest: Content {
    let Cmd: [String]
    let AttachStdin: Bool?
    let AttachStdout: Bool?
    let AttachStderr: Bool?
    let Tty: Bool?
}

struct CreateExecResponse: Content {
    let Id: String
}

// Helper to convert pipes to stdio array
struct Stdio {
    let stdin: FileHandle?
    let stdout: FileHandle?
    let stderr: FileHandle?

    var asArray: [FileHandle?] {
        [stdin, stdout, stderr]
    }
}

struct ExecRoute: RouteCollection {
    let client: ClientContainerProtocol

    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.POST, pattern: "/containers/{id}/exec", use: ExecRoute.createExec(client: client))
        try routes.registerVersionedRoute(.GET, pattern: "/exec/{id}/json", use: ExecRoute.inspectExec(client: client))
        try routes.registerVersionedRoute(.POST, pattern: "/exec/{id}/start", use: ExecRoute.startExec(client: client))
    }

    static func inspectExec(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> Response {
        { req in

            guard let execId = req.parameters.get("id") else {
                throw Abort(.badRequest, reason: "Missing exec ID")
            }

            guard let config = await ExecManager.shared.get(id: execId) else {
                throw Abort(.notFound, reason: "Exec process not found")
            }

            struct ExecInspectResponse: Content {
                let ID: String
                let Running: Bool
                let ExitCode: Int?
                let ProcessConfig: ProcessConfigInfo
                let OpenStdin: Bool
                let OpenStderr: Bool
                let OpenStdout: Bool
                let CanRemove: Bool
                let ContainerID: String
                let DetachKeys: String
                let Pid: Int?

                struct ProcessConfigInfo: Content {
                    let privileged: Bool
                    let user: String
                    let tty: Bool
                    let entrypoint: String
                    let arguments: [String]
                    let workingDir: String
                    let env: [String]
                }
            }

            let response = ExecInspectResponse(
                ID: execId,
                Running: config.running,
                ExitCode: config.exitCode,
                ProcessConfig: ExecInspectResponse.ProcessConfigInfo(
                    privileged: false,
                    user: "",
                    tty: config.tty,
                    entrypoint: config.cmd.first ?? "",
                    arguments: Array(config.cmd.dropFirst()),
                    workingDir: "",
                    env: []
                ),
                OpenStdin: config.attachStdin,
                OpenStderr: config.attachStderr,
                OpenStdout: config.attachStdout,
                CanRemove: true,
                ContainerID: config.containerId,
                DetachKeys: "",
                Pid: nil
            )

            return Response(status: .ok, body: .init(data: try JSONEncoder().encode(response)))
        }
    }

    static func createExec(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> Response {
        { req in

            guard let containerId = req.parameters.get("id") else {
                throw Abort(.badRequest, reason: "Missing container ID")
            }

            guard let container = try await client.getContainer(id: containerId) else {
                throw Abort(.notFound, reason: "Container not found")
            }

            do {
                try client.enforceContainerRunning(container: container)
            } catch {
                throw Abort(.conflict, reason: "Container is not running")
            }

            let body = try req.content.decode(CreateExecRequest.self)

            // there is an error if we provides attachStderr with terminal true
            var attachStderr = body.AttachStderr ?? true
            if body.Tty ?? false {
                attachStderr = false
            }

            let config = ExecManager.ExecConfig(
                containerId: containerId,
                cmd: body.Cmd,
                attachStdin: body.AttachStdin ?? false,
                attachStdout: body.AttachStdout ?? true,
                attachStderr: attachStderr,
                tty: body.Tty ?? false,
                detach: false
            )

            let id = await ExecManager.shared.create(config: config)
            return Response(status: .created, body: .init(data: try JSONEncoder().encode(CreateExecResponse(Id: id))))
        }
    }

    static func startExec(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> Response {
        { req in
            guard let execId = req.parameters.get("id") else {
                throw Abort(.badRequest, reason: "Missing exec ID")
            }

            guard let config = await ExecManager.shared.get(id: execId) else {
                throw Abort(.notFound, reason: "Exec process not found")
            }

            guard let container = try await client.getContainer(id: config.containerId) else {
                throw Abort(.notFound, reason: "Container not found")
            }

            try client.enforceContainerRunning(container: container)

            struct StartExecRequest: Content {
                let Detach: Bool?
                let Tty: Bool?
                let ConsoleSize: [Int]?
            }

            let startRequest = try req.content.decode(StartExecRequest.self)

            let detach = startRequest.Detach ?? false
            let tty = startRequest.Tty ?? config.tty
            let _ = startRequest.ConsoleSize

            req.logger.info("exec/start execId=\(execId) cmd=\(config.cmd) attachStdin=\(config.attachStdin) attachStdout=\(config.attachStdout) attachStderr=\(config.attachStderr) tty=\(tty) detach=\(detach)")

            // Detached mode
            if detach {
                let executable = config.cmd.first!
                let arguments = Array(config.cmd.dropFirst())
                var processConfig = container.configuration.initProcess
                processConfig.executable = executable
                processConfig.arguments = arguments
                processConfig.terminal = tty

                let process = try await ContainerClient().createProcess(
                    containerId: container.id,
                    processId: UUID().uuidString.lowercased(),
                    configuration: processConfig,
                    stdio: [nil, nil, nil]
                )
                try await process.start()
                await ExecManager.shared.markRunning(id: execId)
                return Response(status: .ok)
            }

            // Docker CLI always sends Connection: Upgrade for exec/start, even without stdin
            let connectionHeader = req.headers.first(name: "Connection")?.lowercased()
            let upgradeHeader = req.headers.first(name: "Upgrade")?.lowercased()
            let shouldUpgrade = connectionHeader?.contains("upgrade") == true && upgradeHeader == "tcp"

            req.logger.info("exec/start Connection=\(connectionHeader ?? "nil") Upgrade=\(upgradeHeader ?? "nil") shouldUpgrade=\(shouldUpgrade)")

            guard shouldUpgrade else {
                req.logger.info("exec/start taking HTTP streaming fallback path")
                // Fallback to HTTP streaming mode
                return ConnectionHijackingMiddleware.createDockerStreamingResponse(
                    request: req,
                    ttyEnabled: tty
                ) { streamContinuation in

                    // Safe pipes: raw pipe(2) + dup()
                    let stdinPipe: StdinPipe? = config.attachStdin ? makeStdinPipe() : nil
                    let stdoutPipe: OutputPipe? = config.attachStdout ? makeOutputPipe() : nil
                    let stderrPipe: OutputPipe? = (config.attachStderr && !tty) ? makeOutputPipe() : nil

                    let stdio = Stdio(
                        stdin: stdinPipe?.forProcess,
                        stdout: stdoutPipe?.forProcess,
                        stderr: stderrPipe?.forProcess
                    )

                    let executable = config.cmd.first!
                    let arguments = Array(config.cmd.dropFirst())
                    var processConfig = container.configuration.initProcess
                    processConfig.executable = executable
                    processConfig.arguments = arguments
                    processConfig.terminal = tty

                    let process = try await ContainerClient().createProcess(
                        containerId: container.id,
                        processId: UUID().uuidString.lowercased(),
                        configuration: processConfig,
                        stdio: stdio.asArray
                    )

                    try await process.start()
                    await ExecManager.shared.markRunning(id: execId)
                    req.logger.info("exec/start [HTTP] process started for \(config.cmd)")

                    await withTaskGroup(of: Void.self) { group in
                        // stdout handler
                        if let stdoutHandle = stdoutPipe?.forUs {
                            group.addTask {
                                defer {
                                    try? stdoutHandle.close()
                                }

                                while true {
                                    do {
                                        guard let data = try stdoutHandle.read(upToCount: 8192), !data.isEmpty else {
                                            break  // EOF
                                        }

                                        let bufferSize = min(data.count + (tty ? 0 : 8), 65536)
                                        var buffer = sharedAllocator.buffer(capacity: bufferSize)
                                        buffer.writeDockerFrame(streamType: .stdout, data: data, ttyMode: tty)
                                        streamContinuation.yield(buffer)
                                    } catch {
                                        break
                                    }
                                }
                            }
                        }

                        // stderr handler
                        if let stderrHandle = stderrPipe?.forUs {
                            group.addTask {
                                defer {
                                    try? stderrHandle.close()
                                }

                                while true {
                                    do {
                                        guard let data = try stderrHandle.read(upToCount: 8192), !data.isEmpty else {
                                            break  // EOF
                                        }

                                        let bufferSize = min(data.count + 8, 65536)
                                        var buffer = sharedAllocator.buffer(capacity: bufferSize)
                                        buffer.writeDockerFrame(streamType: .stderr, data: data, ttyMode: tty)
                                        streamContinuation.yield(buffer)
                                    } catch {
                                        break
                                    }
                                }
                            }
                        }

                        // stdin handler for HTTP mode
                        if let stdinFd = stdinPipe?.forUsFd {
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
                        }

                        // Process monitor
                        group.addTask {
                            defer {
                                if let fd = stdoutPipe?.forCleanup { close(fd) }
                                if let fd = stderrPipe?.forCleanup { close(fd) }
                            }

                            do {
                                let exitCode = try await process.wait()
                                req.logger.info("exec/start [HTTP] process exited with code \(exitCode)")
                                await ExecManager.shared.markCompleted(id: execId, exitCode: exitCode)
                            } catch {
                                req.logger.error("exec/start [HTTP] process wait error: \(error)")
                                await ExecManager.shared.markCompleted(id: execId, exitCode: -1)
                            }
                        }

                        for await _ in group {}
                    }

                    req.logger.info("exec/start [HTTP] all tasks done, finishing stream")
                    streamContinuation.finish()
                }
            }
            // Use Docker TCP upgrader for true connection hijacking
            req.logger.info("exec/start taking TCP upgrade path")

            return Response.dockerTCPUpgrade(
                execId: execId,
                ttyEnabled: tty
            ) { channel, tcpHandler in

                // Safe pipes: raw pipe(2) + dup() — no Foundation Pipe auto-close issues
                let stdinPipe: StdinPipe? = config.attachStdin ? makeStdinPipe() : nil
                let stdoutPipe: OutputPipe? = config.attachStdout ? makeOutputPipe() : nil
                let stderrPipe: OutputPipe? = (config.attachStderr && !tty) ? makeOutputPipe() : nil

                let stdio = Stdio(
                    stdin: stdinPipe?.forProcess,
                    stdout: stdoutPipe?.forProcess,
                    stderr: stderrPipe?.forProcess
                )

                if let fd = stdinPipe?.forUsFd {
                    tcpHandler.setStdinFd(fd)
                }

                let executable = config.cmd.first!
                let arguments = Array(config.cmd.dropFirst())

                var processConfig = container.configuration.initProcess
                processConfig.executable = executable
                processConfig.arguments = arguments
                processConfig.terminal = tty

                req.logger.info("exec/start [TCP] creating process for \(config.cmd)")
                let process = try await ContainerClient().createProcess(
                    containerId: container.id,
                    processId: UUID().uuidString.lowercased(),
                    configuration: processConfig,
                    stdio: stdio.asArray
                )

                try await process.start()
                await ExecManager.shared.markRunning(id: execId)
                req.logger.info("exec/start [TCP] process started for \(config.cmd), channel.isActive=\(channel.isActive)")

                // Setup bidirectional communication for interactive sessions
                await withTaskGroup(of: Void.self) { group in
                    // stdout -> channel (container output to client)
                    if let stdoutHandle = stdoutPipe?.forUs {
                        group.addTask {
                            let dispatchIO = DispatchIO(
                                type: .stream,
                                fileDescriptor: stdoutHandle.fileDescriptor,
                                queue: DispatchQueue.global(qos: .userInteractive)
                            ) { error in
                                try? stdoutHandle.close()
                            }

                            // Set up for streaming
                            dispatchIO.setLimit(lowWater: 1)
                            dispatchIO.setLimit(highWater: 4096)

                            // Use a single read operation that processes all available data
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

                                // Start a continuous read operation
                                dispatchIO.read(
                                    offset: 0,
                                    length: Int.max,  // Read all available data
                                    queue: DispatchQueue.global(qos: .userInteractive)
                                ) { done, data, error in

                                    completionLock.lock()
                                    let shouldProcess = !hasCompleted && channel.isActive
                                    completionLock.unlock()

                                    if shouldProcess, let data = data, !data.isEmpty {
                                        channel.eventLoop.execute {
                                            let bufferSize = min(data.count + (tty ? 0 : 8), 65536)
                                            var outputBuffer = channel.allocator.buffer(capacity: bufferSize)
                                            if tty {
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
                            dispatchIO.setLimit(highWater: 1024)

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
                                            let bufferSize = min(data.count + 8, 65536)
                                            var outputBuffer = channel.allocator.buffer(capacity: bufferSize)
                                            outputBuffer.writeDockerFrame(streamType: .stderr, data: Data(data), ttyMode: tty)
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

                    // Process monitor
                    group.addTask {
                        do {
                            let exitCode = try await process.wait()
                            req.logger.info("exec/start [TCP] process exited with code \(exitCode)")
                            await ExecManager.shared.markCompleted(id: execId, exitCode: exitCode)
                        } catch {
                            req.logger.error("exec/start [TCP] process wait error: \(error)")
                            await ExecManager.shared.markCompleted(id: execId, exitCode: -1)
                        }

                        try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms

                        req.logger.info("exec/start [TCP] closing pipes and channel")
                        if let fd = stdoutPipe?.forCleanup { close(fd) }
                        if let fd = stderrPipe?.forCleanup { close(fd) }
                        tcpHandler.setStdinFd(-1)

                        if channel.isActive {
                            _ = channel.eventLoop.submit {
                                channel.close(promise: nil)
                            }
                        }
                    }

                    for await _ in group {}
                }

                req.logger.info("exec/start [TCP] all tasks done")
            }
        }
    }
}
