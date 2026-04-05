import ContainerAPIClient
import ContainerResource
import Containerization
import ContainerizationError
import Foundation
import Vapor

struct NetworkDNSManagerKey: StorageKey {
    typealias Value = NetworkDNSManager
}

/// Manages one CoreDNS container per user network.
///
/// Each DNS container forwards queries to the socktainer DNS server running on the host
/// (reachable at the network's gateway IP on port `dnsPort`), which resolves container
/// hostnames and forwards unknown queries to 1.1.1.1.
actor NetworkDNSManager {
    private let appSupportURL: URL
    private let dnsPort: Int
    private var containerIPs: [String: String] = [:]  // networkId → DNS container IP

    init(appSupportURL: URL, dnsPort: Int = 2054) {
        self.appSupportURL = appSupportURL
        self.dnsPort = dnsPort
    }

    /// Returns the IP of the DNS container for `networkId`, creating it if needed.
    func ensureDNSContainer(networkId: String) async throws -> String {
        if let ip = containerIPs[networkId] {
            return ip
        }

        let containerClient = ContainerClient()
        let containerId = dnsContainerId(for: networkId)

        // Reuse if already running
        if let snapshot = try? await containerClient.get(id: containerId),
            snapshot.status == .running,
            let attachment = snapshot.networks.first(where: { $0.network == networkId })
        {
            let ip = attachment.ipv4Address.address.description
            containerIPs[networkId] = ip
            return ip
        }

        // Get the network's gateway IP for the Corefile
        let networkState = try await ClientNetwork.get(id: networkId)
        guard case .running(_, let networkStatus) = networkState else {
            throw ContainerizationError(
                .invalidState, message: "network \(networkId) is not running")
        }
        let gatewayIP = networkStatus.ipv4Gateway.description

        // Write Corefile to a host directory that will be virtiofs-mounted
        let corefileDir = appSupportURL.appendingPathComponent("dns/\(networkId)")
        try FileManager.default.createDirectory(
            at: corefileDir, withIntermediateDirectories: true)
        let corefileURL = corefileDir.appendingPathComponent("Corefile")
        let corefile = """
            . {
                forward . \(gatewayIP):\(dnsPort)
                cache 10
                errors
            }
            """
        try corefile.write(to: corefileURL, atomically: true, encoding: .utf8)

        // Build container configuration
        let platform = Platform.current
        let imageRef = "coredns/coredns:latest"
        let image = try await ClientImage.fetch(reference: imageRef, platform: platform)
        _ = try await image.getCreateSnapshot(platform: platform)
        let imageDesc = ImageDescription(reference: imageRef, descriptor: image.descriptor)

        let processConfig = ProcessConfiguration(
            executable: "/coredns",
            arguments: ["-conf", "/etc/coredns/Corefile"],
            environment: [],
            workingDirectory: "/",
            terminal: false,
            user: .id(uid: 0, gid: 0)
        )

        var config = ContainerConfiguration(id: containerId, image: imageDesc, process: processConfig)
        config.labels = [
            "socktainer.role": "dns",
            "socktainer.network": networkId,
        ]
        config.mounts = [
            .init(type: .virtiofs, source: corefileDir.path, destination: "/etc/coredns", options: [])
        ]
        config.networks = [
            AttachmentConfiguration(
                network: networkId,
                options: AttachmentOptions(hostname: "dns-\(networkId)"))
        ]
        // Use the vmnet gateway DNS directly for CoreDNS itself (not itself to avoid loops)
        config.dns = ContainerConfiguration.DNSConfiguration(
            nameservers: [gatewayIP],
            domain: nil,
            searchDomains: [],
            options: []
        )

        let kernel = try await ClientKernel.getDefaultKernel(for: .current)

        // Delete stale container if it exists in stopped/unknown state
        try? await containerClient.delete(id: containerId)

        try await containerClient.create(configuration: config, options: .default, kernel: kernel)
        try await startContainer(id: containerId, containerClient: containerClient)

        let snapshot = try await containerClient.get(id: containerId)
        guard let attachment = snapshot.networks.first(where: { $0.network == networkId }) else {
            throw ContainerizationError(
                .invalidState, message: "DNS container for network \(networkId) has no attachment")
        }

        let ip = attachment.ipv4Address.address.description
        containerIPs[networkId] = ip
        return ip
    }

    private func startContainer(id: String, containerClient: ContainerClient) async throws {
        let io = try ProcessIO.create(tty: false, interactive: false, detach: true)
        defer { try? io.close() }
        do {
            let process = try await containerClient.bootstrap(id: id, stdio: io.stdio)
            try await process.start()
            try io.closeAfterStart()
        } catch {
            try? await containerClient.stop(id: id)
            try? await containerClient.delete(id: id)
            throw ContainerizationError(
                .internalError, message: "failed to start DNS container: \(error)")
        }
    }

    private func dnsContainerId(for networkId: String) -> String {
        "socktainer-dns-\(networkId)"
    }
}
