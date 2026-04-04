import ContainerAPIClient
import Foundation
import Vapor

struct ContainerStartRoute: RouteCollection {
    let client: ClientContainerProtocol
    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.POST, pattern: "/containers/{id}/start", use: ContainerStartRoute.handler(client: client))
    }
}

struct ContainerStartQuery: Content {
    /// Override the key sequence for detaching a container
    let detachKeys: String?
}

extension ContainerStartRoute {
    static func handler(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> HTTPStatus {
        { req in

            guard let id = req.parameters.get("id") else {
                throw Abort(.badRequest, reason: "Missing container ID")
            }

            let query = try req.query.decode(ContainerStartQuery.self)
            let detachKeys = query.detachKeys

            var containerLabels: [String: String] = [:]
            var containerImage: String = ""

            do {
                guard let container = try await client.getContainer(id: id) else {
                    throw Abort(.notFound, reason: "No such container: \(id)")
                }

                containerLabels = container.configuration.labels
                containerImage = container.configuration.image.reference

                if container.status == .running {
                    req.logger.debug("Container \(id) is already running")
                } else if let pendingAttach = await AttachManager.shared.consumePipes(id: id) {
                    // /attach prepared pipes for us — bootstrap with them so output flows to attach stream
                    // closeOnDealloc: false — bootstrap dup2's these fds to virtio channels;
                    // closing them after would kill the container's I/O
                    let stdio: [FileHandle?] = [
                        nil,
                        pendingAttach.stdoutWriteFd >= 0 ? FileHandle(fileDescriptor: pendingAttach.stdoutWriteFd, closeOnDealloc: false) : nil,
                        pendingAttach.stderrWriteFd >= 0 ? FileHandle(fileDescriptor: pendingAttach.stderrWriteFd, closeOnDealloc: false) : nil,
                    ]

                    let process = try await ContainerClient().bootstrap(id: id, stdio: stdio)
                    try await process.start()
                    await AttachManager.shared.setProcess(id: id, process: process)
                    req.logger.info("Started container \(id) with attach pipes")
                } else {
                    // No pending attach — start normally (no output capture)
                    try await client.start(id: id, detachKeys: detachKeys)
                    req.logger.debug("Started container \(id)")
                }

            } catch {
                let errorMessage = error.localizedDescription
                let isAlreadyRunning =
                    errorMessage.contains("booted") || errorMessage.contains("expected to be in created state") || errorMessage.contains("invalidState")
                    || errorMessage.contains("already running")

                guard isAlreadyRunning else {
                    req.logger.error("Failed to start container \(id): \(error)")
                    throw Abort(.internalServerError, reason: "Failed to start container: \(error)")
                }
                req.logger.debug("Container \(id) was already running or bootstrapped")
            }

            let broadcaster = req.application.storage[EventBroadcasterKey.self]!
            let event = DockerEvent.simpleEvent(id: id, type: "container", status: "start", image: containerImage, labels: containerLabels)
            await broadcaster.broadcast(event)

            return .noContent
        }
    }
}
