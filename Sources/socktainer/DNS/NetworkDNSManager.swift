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

    /// In-flight creation tasks. Without this, multiple concurrent container creations
    /// would race: each caller checks `containerIPs == nil`, all proceed past that
    /// check before any finishes (actor releases lock at each `await`), and all try
    /// to create the same DNS container simultaneously.
    private var pendingCreation: [String: Task<String, Error>] = [:]

    init(appSupportURL: URL, dnsPort: Int = 2054) {
        self.appSupportURL = appSupportURL
        self.dnsPort = dnsPort
    }

    /// Returns the IP of the DNS container for `networkId`, creating it if needed.
    func ensureDNSContainer(networkId: String) async throws -> String {
        // Fast path: already resolved
        if let ip = containerIPs[networkId] { return ip }

        // Coalesce concurrent callers: if a creation is already in-flight, wait for it.
        // Both checks + task assignment happen synchronously (no await between them),
        // so subsequent callers always find the pending task before it completes.
        if let pending = pendingCreation[networkId] {
            return try await pending.value
        }

        // Capture values before any suspension point
        let appSupportURL = self.appSupportURL
        let dnsPort = self.dnsPort

        let task = Task<String, Error> {
            try await Self.createDNSContainerWork(
                networkId: networkId,
                appSupportURL: appSupportURL,
                dnsPort: dnsPort
            )
        }
        pendingCreation[networkId] = task

        do {
            let ip = try await task.value
            containerIPs[networkId] = ip
            pendingCreation.removeValue(forKey: networkId)
            return ip
        } catch {
            pendingCreation.removeValue(forKey: networkId)
            throw error
        }
    }

    /// All container creation logic is in a static (non-actor-isolated) function
    /// so it runs outside the actor's executor, preventing deadlock with the
    /// Task created above.
    private static func createDNSContainerWork(
        networkId: String,
        appSupportURL: URL,
        dnsPort: Int
    ) async throws -> String {
        let containerClient = ContainerClient()
        let containerId = "socktainer-dns-\(networkId)"

        // Reuse if already running
        if let snapshot = try? await containerClient.get(id: containerId),
            snapshot.status == .running,
            let attachment = snapshot.networks.first(where: { $0.network == networkId })
        {
            return attachment.ipv4Address.address.description
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
        try FileManager.default.createDirectory(at: corefileDir, withIntermediateDirectories: true)
        let corefileURL = corefileDir.appendingPathComponent("Corefile")
        let corefile = """
            . {
                forward . \(gatewayIP):\(dnsPort)
                cache 10
                errors
                log
            }
            """
        try corefile.write(to: corefileURL, atomically: true, encoding: .utf8)

        // Build container configuration
        let platform = Platform.current
        let imageRef = try ClientImage.normalizeReference("coredns/coredns:latest")
        let image = try await ClientImage.fetch(reference: imageRef, platform: platform)
        _ = try await image.getCreateSnapshot(platform: platform)

        let processConfig = ProcessConfiguration(
            executable: "/coredns",
            arguments: ["-conf", "/etc/coredns/Corefile"],
            environment: [],
            workingDirectory: "/",
            terminal: false,
            user: .id(uid: 0, gid: 0)
        )

        var config = ContainerConfiguration(id: containerId, image: image.description, process: processConfig)
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
        // Use the vmnet gateway DNS directly so CoreDNS doesn't loop back to itself
        config.dns = ContainerConfiguration.DNSConfiguration(
            nameservers: [gatewayIP],
            domain: nil,
            searchDomains: [],
            options: []
        )

        let kernel = try await ClientKernel.getDefaultKernel(for: .current)

        // Clean up any stale container (stop first if running, then delete)
        if let existing = try? await containerClient.get(id: containerId) {
            if existing.status == .running {
                try? await containerClient.stop(id: containerId)
            }
            try? await containerClient.delete(id: containerId)
        }

        try await containerClient.create(configuration: config, options: .default, kernel: kernel)
        try await startDNSContainer(id: containerId, containerClient: containerClient)

        let snapshot = try await containerClient.get(id: containerId)
        guard let attachment = snapshot.networks.first(where: { $0.network == networkId }) else {
            throw ContainerizationError(
                .invalidState, message: "DNS container for network \(networkId) has no attachment")
        }

        return attachment.ipv4Address.address.description
    }

    private static func startDNSContainer(id: String, containerClient: ContainerClient) async throws {
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
}
