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

            // Register DNS names now that the container is running and has an IP.
            // Names were stored in the label at create time (includes Compose service aliases).
            if let dnsServer = req.application.storage[SocktainerDNSServerKey.self],
                containerLabels["socktainer.role"] != "dns",
                let namesLabel = containerLabels["socktainer.dns.names"],
                let snapshot = try? await ContainerClient().get(id: id),
                let firstAttachment = snapshot.networks.first
            {
                let ip = firstAttachment.ipv4Address.address.description
                for name in namesLabel.split(separator: ",").map(String.init) where !name.isEmpty {
                    dnsServer.register(hostname: name, ip: ip)
                }
            }

            // Start healthcheck loop if container has a stored healthcheck config
            if let hcManager = req.application.storage[HealthCheckManagerKey.self],
                let hcJson = containerLabels["socktainer.healthcheck"],
                let hcConfig = try? JSONDecoder().decode(HealthcheckConfig.self, from: Data(hcJson.utf8))
            {
                await hcManager.start(containerId: id, config: hcConfig)
            }

            let broadcaster = req.application.storage[EventBroadcasterKey.self]!
            let event = DockerEvent.simpleEvent(id: id, type: "container", status: "start", image: containerImage, labels: containerLabels)
            await broadcaster.broadcast(event)

            return .noContent
        }
    }
}
