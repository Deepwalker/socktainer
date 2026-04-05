import ContainerAPIClient
import Vapor

struct ContainerDeleteRoute: RouteCollection {
    let client: ClientContainerProtocol
    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.DELETE, pattern: "/containers/{id}", use: ContainerDeleteRoute.handler(client: client))
    }

}

extension ContainerDeleteRoute {
    static func handler(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> HTTPStatus {
        { req in
            guard let id = req.parameters.get("id") else {
                throw Abort(.badRequest, reason: "Missing container ID")
            }

            // Unregister DNS entries before deletion.
            // Use socktainer.dns.names label (written at create time) to catch all aliases
            // including Docker Compose service names, not just attachment.hostname.
            if let dnsServer = req.application.storage[SocktainerDNSServerKey.self],
                let container = try? await ContainerClient().get(id: id)
            {
                if let namesLabel = container.configuration.labels["socktainer.dns.names"] {
                    for name in namesLabel.split(separator: ",").map(String.init) where !name.isEmpty {
                        dnsServer.unregister(hostname: name)
                    }
                } else {
                    for attachment in container.networks {
                        dnsServer.unregister(hostname: attachment.hostname)
                    }
                }
            }

            // if running, stop it first
            if let container = try await client.getContainer(id: id),
                container.status == .running
            {
                try await client.stop(id: id, signal: nil, timeout: nil)
            }
            try await client.delete(id: id)

            let broadcaster = req.application.storage[EventBroadcasterKey.self]!

            let event = DockerEvent.simpleEvent(id: id, type: "container", status: "remove")

            await broadcaster.broadcast(event)

            return .ok

        }
    }
}
