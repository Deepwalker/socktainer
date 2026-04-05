import ContainerAPIClient
import ContainerBuild
import ContainerImagesServiceClient
import Containerization
import ContainerizationError
import ContainerizationOCI
import ContainerizationOS
import Foundation
import NIO
import TerminalProgress
import Vapor

struct BuildRoute: RouteCollection {

    let client: ClientContainerProtocol
    let builderClient: ClientBuilderProtocol

    init(client: ClientContainerProtocol, builderClient: ClientBuilderProtocol) {
        self.client = client
        self.builderClient = builderClient
    }

    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.POST, pattern: "/build", use: BuildRoute.handler(client: client, builderClient: builderClient))

    }

}

struct RESTBuildQuery: Vapor.Content {
    var dockerfile: String?
    var t: String?  // tag
    var extrahosts: String?  // path to extra hosts file
    var remote: String?  // remote URL to build context
    var q: Bool?  // quiet
    var nocache: Bool?  // no cache
    var cachefrom: String?  // cache from
    var pull: String?
    var rm: Bool?  // remove intermediate containers
    var forcerm: Bool?  // always remove intermediate containers
    var memory: Int?  // memory limit in bytes
    var memswap: Int?  // total memory (memory + swap); -1 to disable swap
    var cpushares: Int?  // CPU shares (relative weight)
    var cpusetcpus: String?  // CPUs in which to allow execution
    var cpuperiod: Int?  // limit CPU CFS period
    var cpuquota: Int?  // limit CPU CFS quota
    var buildargs: String?  // build arguments
    var shmsize: Int?  // size of /dev/shm in bytes
    var squash: Bool?  // squash the resulting image
    var labels: String?  // labels to set on the image
    var networkmode: String?  // networking mode for the RUN instructions during build
    var platform: String?  // target platform for build
    var target: String?  // target stage to build
    var outputs: String?  // output destination
    var version: String?  // API version

    init() {
        self.dockerfile = "Dockerfile"
        self.q = false
        self.nocache = false
        self.rm = true
        self.forcerm = false
        self.platform = "linux/arm64"
        self.target = ""
        self.outputs = ""
        self.version = "1"
    }
}

extension BuildRoute {
    static func handler(client: ClientContainerProtocol, builderClient: ClientBuilderProtocol) -> @Sendable (Request) async throws -> Response {
        { req in
            var query = try req.query.decode(RESTBuildQuery.self)

            // Apply Docker API defaults if not provided
            if query.dockerfile == nil { query.dockerfile = "Dockerfile" }
            if query.q == nil { query.q = false }
            if query.nocache == nil { query.nocache = false }
            if query.rm == nil { query.rm = true }
            if query.forcerm == nil { query.forcerm = false }
            if query.platform == nil { query.platform = "" }
            if query.target == nil { query.target = "" }
            if query.outputs == nil { query.outputs = "" }
            if query.version == nil { query.version = "1" }

            // Extract values with Docker-compliant defaults
            let dockerfile = query.dockerfile!
            let targetImageName = query.t ?? UUID().uuidString.lowercased()
            let quiet = query.q!
            let noCache = query.nocache!
            let pull = query.pull.map { ["1", "true", "yes", "on"].contains($0.lowercased()) } ?? false
            let target = query.target!
            let platform = query.platform!
            let memory = query.memory ?? 2_048_000_000  // 2GB default

            do {
                try await builderClient.ensureReachable(
                    timeout: .seconds(3),
                    retryInterval: .milliseconds(250),
                    logger: req.logger
                )
            } catch {
                throw Abort(.serviceUnavailable, reason: "BuildKit builder is not running or reachable: \(error.localizedDescription)")
            }

            let perfClock = ContinuousClock()
            let perfStart = perfClock.now
            func perfMs() -> Int64 {
                let d = perfClock.now - perfStart
                return d.components.seconds * 1000 + Int64(d.components.attoseconds / 1_000_000_000_000_000)
            }

            req.logger.info("[build-perf] \(perfMs())ms ensureReachable done")

            let contextDir: String
            let buildUUID = UUID().uuidString
            let appSupportDir = try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
                .appendingPathComponent("com.apple.container/builder")
            let tempContextDir = appSupportDir.appendingPathComponent(buildUUID)

            do {
                try FileManager.default.createDirectory(at: tempContextDir, withIntermediateDirectories: true, attributes: nil)

                let hasBody = req.body.data != nil
                    || req.headers.first(name: "transfer-encoding")?.lowercased() == "chunked"
                    || (req.headers.first(name: "content-length").flatMap(Int.init) ?? 0) > 0

                if hasBody {
                    let tarPath = tempContextDir.appendingPathComponent("context.tar")
                    var totalBytesWritten = 0

                    // Write body to tar file
                    let fd = open(tarPath.path, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
                    guard fd >= 0 else {
                        throw Abort(.internalServerError, reason: "Failed to create tar file")
                    }

                    do {
                        if let bodyData = req.body.data {
                            bodyData.withUnsafeReadableBytes { ptr in
                                guard let base = ptr.baseAddress else { return }
                                var remaining = ptr.count
                                var offset = 0
                                while remaining > 0 {
                                    let n = Darwin.write(fd, base + offset, remaining)
                                    if n < 0 { if errno == EINTR { continue }; break }
                                    if n == 0 { break }
                                    offset += n
                                    remaining -= n
                                }
                                totalBytesWritten = offset
                            }
                        } else {
                            for try await chunk in req.body {
                                let ok = chunk.withUnsafeReadableBytes { ptr -> Bool in
                                    guard let base = ptr.baseAddress else { return true }
                                    var remaining = ptr.count
                                    var offset = 0
                                    while remaining > 0 {
                                        let n = Darwin.write(fd, base + offset, remaining)
                                        if n < 0 { if errno == EINTR { continue }; return false }
                                        if n == 0 { return false }
                                        offset += n
                                        remaining -= n
                                    }
                                    totalBytesWritten += ptr.count
                                    return true
                                }
                                if !ok { break }
                            }
                        }
                        close(fd)
                    } catch {
                        close(fd)
                        throw Abort(.badRequest, reason: "Failed reading request body: \(error.localizedDescription)")
                    }

                    req.logger.info("[build-perf] \(perfMs())ms body received (\(totalBytesWritten) bytes)")

                    if totalBytesWritten > 0 {
                        let extractDir = tempContextDir.appendingPathComponent("context")
                        try FileManager.default.createDirectory(at: extractDir, withIntermediateDirectories: true, attributes: nil)

                        // macOS bsdtar auto-detects gzip/plain tar via libarchive — no need to check Content-Type
                        let extractProc = Process()
                        let stderrPipe = Pipe()

                        extractProc.executableURL = URL(fileURLWithPath: "/usr/bin/tar")
                        extractProc.arguments = ["xf", tarPath.path, "-C", extractDir.path]

                        extractProc.standardOutput = FileHandle.nullDevice
                        extractProc.standardError = stderrPipe
                        try extractProc.run()
                        extractProc.waitUntilExit()
                        let stderrText = String(data: stderrPipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

                        req.logger.info("[build-perf] \(perfMs())ms tar extracted")

                        if extractProc.terminationStatus == 1 {
                            // exit 1 = BSD tar warning (e.g. truncated trailing block) — extraction succeeded
                            req.logger.warning("tar exited with code 1 (non-fatal): \(stderrText)")
                        } else if extractProc.terminationStatus > 1 {
                            let debugCopy = "/tmp/socktainer-debug-\(buildUUID).tar"
                            try? FileManager.default.copyItem(atPath: tarPath.path, toPath: debugCopy)
                            req.logger.error("tar extraction failed, debug copy: \(debugCopy)")
                            throw Abort(.badRequest, reason: "tar extraction failed (exit \(extractProc.terminationStatus)): \(stderrText)")
                        }

                        try? FileManager.default.removeItem(at: tarPath)
                        contextDir = extractDir.path
                    } else {
                        req.logger.warning("No data received in request body")
                        contextDir = "."
                    }
                } else {
                    req.logger.warning("No build context provided in request body")
                    contextDir = "."
                }
            } catch {
                try? FileManager.default.removeItem(at: tempContextDir)
                throw error
            }

            // Parse build arguments — Docker API sends JSON: {"KEY":"VALUE",...}
            // BuildKit expects ["KEY=VALUE", ...]
            let buildArgs: [String] = {
                guard let buildArgsString = query.buildargs,
                      let data = buildArgsString.data(using: .utf8),
                      let dict = try? JSONSerialization.jsonObject(with: data) as? [String: String]
                else { return [] }
                return dict.map { "\($0.key)=\($0.value)" }
            }()

            // Parse labels — same JSON format
            let labels: [String] = {
                guard let labelsString = query.labels,
                      let data = labelsString.data(using: .utf8),
                      let dict = try? JSONSerialization.jsonObject(with: data) as? [String: String]
                else { return [] }
                return dict.map { "\($0.key)=\($0.value)" }
            }()

            // Create streaming response for build output
            let body = Response.Body { writer in
                Task.detached {
                    do {
                        try await BuildRoute.performBuild(
                            dockerfile: dockerfile,
                            contextDir: contextDir,
                            targetImageName: targetImageName,
                            buildArgs: buildArgs,
                            labels: labels,
                            noCache: noCache,
                            pull: pull,
                            target: target,
                            platform: platform,
                            memory: memory,
                            quiet: quiet,
                            builderClient: builderClient,
                            writer: writer,
                            logger: req.logger
                        )

                        // Clean up temporary context directory if it was created
                        if contextDir != "." {
                            try? FileManager.default.removeItem(at: tempContextDir)
                        }
                    } catch {
                        req.logger.error("Build failed: \(error)")

                        // Extract error message - prioritize ContainerizationError message
                        let errorMessage: String
                        if error is ContainerizationError {
                            // Use string interpolation to get ContainerizationError's description
                            errorMessage = "\(error)"
                        } else {
                            errorMessage = error.localizedDescription
                        }

                        // Docker API compliant error response
                        let errorDetail: [String: Any] = [
                            "message": errorMessage
                        ]

                        let errorResponse: [String: Any] = [
                            "errorDetail": errorDetail,
                            "error": errorMessage,
                        ]

                        if let jsonData = try? JSONSerialization.data(withJSONObject: errorResponse),
                            let jsonString = String(data: jsonData, encoding: .utf8)
                        {
                            _ = writer.write(.buffer(ByteBuffer(string: jsonString + "\n")))
                        } else {
                            let fallbackError = """
                                {"errorDetail":{"message":"Build failed"},"error":"Build failed"}

                                """
                            _ = writer.write(.buffer(ByteBuffer(string: fallbackError)))
                        }

                        // Clean up temporary context directory on error
                        if contextDir != "." {
                            try? FileManager.default.removeItem(at: tempContextDir)
                        }
                        _ = writer.write(.end)
                    }
                }
            }

            return Response(
                status: .ok,
                headers: [
                    "Content-Type": "application/json",
                    "Transfer-Encoding": "chunked",
                ],
                body: body
            )
        }
    }

    private static func performBuild(
        dockerfile: String,
        contextDir: String,
        targetImageName: String,
        buildArgs: [String],
        labels: [String],
        noCache: Bool,
        pull: Bool,
        target: String,
        platform: String,
        memory: Int,
        quiet: Bool,
        builderClient: ClientBuilderProtocol,
        writer: BodyStreamWriter,
        logger: Logger
    ) async throws {

        // Helper function to send Docker API compliant streaming messages
        @Sendable func sendStreamMessage(_ message: String) {
            // Preserve the original message with its formatting
            let streamResponse: [String: Any] = ["stream": message + "\n"]
            if let jsonData = try? JSONSerialization.data(withJSONObject: streamResponse),
                let jsonString = String(data: jsonData, encoding: .utf8)
            {
                let result = writer.write(.buffer(ByteBuffer(string: jsonString + "\n")))

                // Log write failures for debugging but don't crash
                result.whenFailure { error in
                    logger.debug("BuildRoute: Write failed - \(error)")
                }
            }
        }

        func sendProgressMessage(id: String, status: String, progressDetail: [String: Any]? = nil) {
            var response: [String: Any] = [
                "id": id,
                "status": status,
            ]
            if let detail = progressDetail {
                response["progressDetail"] = detail
            }

            if let jsonData = try? JSONSerialization.data(withJSONObject: response),
                let jsonString = String(data: jsonData, encoding: .utf8)
            {
                let result = writer.write(.buffer(ByteBuffer(string: jsonString + "\n")))
                result.whenFailure { error in
                    logger.debug("BuildRoute: Progress message write failed - \(error)")
                }
            }
        }

        let buildClock = ContinuousClock()
        let buildStart = buildClock.now

        func elapsed() -> String {
            let ms = (buildClock.now - buildStart).components.seconds * 1000
                + Int64((buildClock.now - buildStart).components.attoseconds / 1_000_000_000_000_000)
            return "\(ms)ms"
        }

        sendStreamMessage("Step 1/1 : Starting build for \(targetImageName)")

        let timeout: Duration = .seconds(300)

        logger.info("[build-perf] \(elapsed()) connecting to builder")
        let builder = try await builderClient.connect(
            timeout: timeout,
            retryInterval: .seconds(1),
            logger: logger
        )
        logger.info("[build-perf] \(elapsed()) builder connected")
        sendStreamMessage(" ---> Connected to builder (\(elapsed()))")

        let dockerfilePath = URL(fileURLWithPath: contextDir).appendingPathComponent(dockerfile).path

        guard let dockerfileData = try? Data(contentsOf: URL(filePath: dockerfilePath)) else {
            throw ContainerizationError(.invalidArgument, message: "Dockerfile does not exist at path: \(dockerfilePath)")
        }

        let builderExportPath = try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
            .appendingPathComponent("com.apple.container/builder")
        let buildID = UUID().uuidString
        let tempURL = builderExportPath.appendingPathComponent(buildID)
        try FileManager.default.createDirectory(at: tempURL, withIntermediateDirectories: true, attributes: nil)

        let imageName: String = try {
            let parsedReference = try Reference.parse(targetImageName)
            parsedReference.normalize()
            return parsedReference.description
        }()

        let exports: [Builder.BuildExport] = try ["type=oci"].map { output in
            var exp = try Builder.BuildExport(from: output)
            if exp.destination == nil {
                exp.destination = tempURL.appendingPathComponent("out.tar")
            }
            return exp
        }

        let platforms: Set<Platform> = {
            guard platform.isEmpty else {
                return [try! Platform(from: platform)]
            }
            return [try! Platform(from: "linux/\(Arch.hostArchitecture().rawValue)")]
        }()

        let config = ContainerBuild.Builder.BuildConfig(
            buildID: buildID,
            contentStore: RemoteContentStoreClient(),
            buildArgs: buildArgs,
            secrets: [:],
            contextDir: contextDir,
            dockerfile: dockerfileData,
            hiddenDockerDir: nil,
            labels: labels,
            noCache: noCache,
            platforms: [Platform](platforms),
            terminal: nil,
            tags: [imageName],
            target: target,
            quiet: quiet,
            exports: exports,
            cacheIn: [],
            cacheOut: [],
            pull: pull
        )

        logger.info("[build-perf] \(elapsed()) starting builder.build()")
        sendStreamMessage(" ---> Starting build (\(elapsed()))")

        try await builder.build(config)

        logger.info("[build-perf] \(elapsed()) builder.build() done")
        sendStreamMessage(" ---> Build done (\(elapsed()))")

        let destPath = tempURL.appendingPathComponent("out.tar")
        guard FileManager.default.fileExists(atPath: destPath.path) else {
            logger.error("Output image not found at: \(destPath.path)")
            throw ContainerizationError(.unknown, message: "Build completed but no output image found at \(destPath.path)")
        }

        logger.info("[build-perf] \(elapsed()) loading image")
        let loaded = try await ClientImage.load(from: destPath.absolutePath())
        logger.info("[build-perf] \(elapsed()) image loaded, unpacking \(loaded.images.count) images")

        for image in loaded.images {
            try await image.unpack(platform: nil, progressUpdate: { _ in })
        }

        logger.info("[build-perf] \(elapsed()) unpack done")
        sendStreamMessage("Successfully built \(imageName) (\(elapsed()) total)")

        _ = writer.write(.end)
    }
}
