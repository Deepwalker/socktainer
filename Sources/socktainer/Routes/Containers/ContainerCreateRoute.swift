import ContainerAPIClient
import ContainerNetworkService
import ContainerResource
import Containerization
import ContainerizationError
import ContainerizationExtras
import Foundation
import Vapor

struct ContainerCreateRoute: RouteCollection {
    let client: ClientContainerProtocol
    func boot(routes: RoutesBuilder) throws {
        try routes.registerVersionedRoute(.POST, pattern: "/containers/create", use: ContainerCreateRoute.handler(client: client))
    }

}

struct ContainerCreateQuery: Content {
    var name: String?
    var platform: String?
}
struct CreateContainerRequest: Content {
    let Image: String
    let Hostname: String?
    let Domainname: String?
    let User: String?
    let AttachStdin: Bool?
    let AttachStdout: Bool?
    let AttachStderr: Bool?
    let PortSpecs: [String]?
    let Tty: Bool?
    let OpenStdin: Bool?
    let StdinOnce: Bool?
    let Env: [String]?
    let Cmd: [String]?
    let Healthcheck: HealthcheckConfig?
    let ArgsEscaped: Bool?
    let Entrypoint: [String]?
    let Volumes: [String: EmptyObject]?
    let WorkingDir: String?
    let MacAddress: String?
    let OnBuild: [String]?
    let NetworkDisabled: Bool?
    let ExposedPorts: [String: EmptyObject]?
    let StopSignal: String?
    let StopTimeout: Int?
    let HostConfig: HostConfig?
    let Labels: [String: String]?
    let Shell: [String]?
    let NetworkingConfig: ContainerNetworkSettings?
}

extension ContainerCreateRoute {
    static func handler(client: ClientContainerProtocol) -> @Sendable (Request) async throws -> RESTContainerCreate {
        { req in
            let query = try req.query.decode(ContainerCreateQuery.self)

            let containerName = query.name

            // use platform "" if not provided
            let containerPlatform = (query.platform?.isEmpty == false) ? query.platform! : "linux/\(Arch.hostArchitecture().rawValue)"

            let bodyData = try await req.body.collect().get()!
            let body = try JSONDecoder().decode(CreateContainerRequest.self, from: bodyData.getData(at: 0, length: bodyData.readableBytes)!)

            req.logger.info("Creating container for image: \(body.Image)")

            let id = Utility.createContainerID(name: containerName)
            try Utility.validEntityName(id)

            // Validate the requested platform only if provided
            let requestedPlatform = try Platform(from: containerPlatform)

            // Try to find the image with different reference variants
            let imageRef: String
            var foundLocally = false

            // First, try the exact reference as provided
            do {
                _ = try await ClientImage.get(reference: body.Image)
                imageRef = body.Image
                foundLocally = true
                req.logger.debug("Found image with exact reference: \(imageRef)")
            } catch {
                // If not found, try adding :latest tag if no tag is present
                let imageWithLatest = body.Image.contains(":") ? body.Image : "\(body.Image):latest"
                do {
                    _ = try await ClientImage.get(reference: imageWithLatest)
                    imageRef = imageWithLatest
                    foundLocally = true
                    req.logger.debug("Found image with :latest tag: \(imageRef)")
                } catch {
                    // Not found locally, will need to normalize and pull
                    imageRef = try ClientImage.normalizeReference(body.Image)
                    req.logger.debug("Image not found locally, normalized reference for pull: \(imageRef)")
                }
            }

            if !foundLocally {
                throw Abort(.notFound, reason: "No such image: \(body.Image)")
            }

            req.logger.info("create: fetching image \(imageRef)")
            let img = try await ClientImage.fetch(
                reference: imageRef,
                platform: requestedPlatform,
            )

            // Unpack a fetched image before use
            req.logger.info("create: getCreateSnapshot for image")
            try await img.getCreateSnapshot(
                platform: requestedPlatform
            )

            req.logger.info("create: getting default kernel")
            let kernel = try await ClientKernel.getDefaultKernel(for: .current)

            req.logger.info("create: fetching init image")
            let initImage = try await ClientImage.fetch(
                reference: ClientImage.initImageRef, platform: .current
            )

            req.logger.info("create: getCreateSnapshot for init image")
            _ = try await initImage.getCreateSnapshot(
                platform: .current)

            let imageConfig = try await img.config(for: requestedPlatform).config

            let defaultUser: ProcessConfiguration.User = {
                if let u = imageConfig?.user {
                    return .raw(userString: u)
                }
                return .id(uid: 0, gid: 0)
            }()

            let workingDirectory = imageConfig?.workingDir ?? "/"

            let imageConfigEnvironment = imageConfig?.env ?? []
            let requestedEnvironment = body.Env ?? []
            // merge environment variables, with request taking precedence
            let mergedEnv = try Parser.allEnv(imageEnvs: imageConfigEnvironment, envFiles: [], envs: requestedEnvironment)

            let publishedPorts: [PublishPort]
            do {
                publishedPorts = try convertPortBindings(
                    from: body.HostConfig?.PortBindings ?? [:]
                )
            } catch {
                req.logger.error("Failed to allocate ports: \(error)")
                throw Abort(.internalServerError, reason: "Failed to allocate ports: \(error)")
            }

            // Handle Entrypoint and Cmd from request, following Docker semantics
            var commandLine: [String] = []

            // Determine the entrypoint to use
            let entrypoint: [String]
            if let requestEntrypoint = body.Entrypoint {
                // If entrypoint is explicitly provided (even if empty), use it
                entrypoint = requestEntrypoint
            } else if let imageEntrypoint = imageConfig?.entrypoint {
                // Otherwise use image's entrypoint
                entrypoint = imageEntrypoint
            } else {
                // No entrypoint specified
                entrypoint = []
            }

            // Determine the command to use
            let command: [String]
            if let requestCmd = body.Cmd {
                // If cmd is explicitly provided but empty, use image's cmd
                command = requestCmd.isEmpty ? (imageConfig?.cmd ?? []) : requestCmd
            } else if body.Entrypoint != nil {
                // If entrypoint was explicitly overridden, don't use image's cmd
                command = []
            } else {
                // Use image's cmd
                command = imageConfig?.cmd ?? []
            }

            // Build final command line
            commandLine.append(contentsOf: entrypoint)
            commandLine.append(contentsOf: command)

            // Use working directory from request if provided and not empty, otherwise from image config
            let finalWorkingDirectory = (body.WorkingDir?.isEmpty == false) ? body.WorkingDir! : workingDirectory

            // Handle user from request if provided
            let finalUser: ProcessConfiguration.User = {
                if let requestUser = body.User {
                    return .raw(userString: requestUser)
                }
                return defaultUser
            }()

            // Ensure we have a valid executable
            guard let executable = commandLine.first, !executable.isEmpty else {
                req.logger.error("No executable specified for container")
                throw Abort(.badRequest, reason: "No executable specified for container. Image must specify ENTRYPOINT or CMD, or request must provide Entrypoint or Cmd.")
            }

            // For Apple Container compatibility, we ignore attach flags during creation
            // Containers are always created in detached mode and can be attached to later
            // TODO: Store attach flags (AttachStdin, AttachStdout, AttachStderr) in container metadata
            // for use when container is started via /start endpoint
            let processConfig = ProcessConfiguration(
                executable: executable,
                arguments: commandLine.dropFirst().map { String($0) },
                environment: mergedEnv,
                workingDirectory: finalWorkingDirectory,
                terminal: body.Tty ?? false,
                user: finalUser,
            )

            var containerConfiguration = ContainerConfiguration(id: id, image: img.description, process: processConfig)
            containerConfiguration.platform = requestedPlatform

            // Enable Rosetta when running amd64 images if on arm64 host
            if Platform.current.architecture == "arm64" && requestedPlatform.architecture == "amd64" {
                containerConfiguration.rosetta = true
            }

            // Handle hostname: prefer explicit Hostname, then first alias from EndpointsConfig, then fallback
            let defaultHostname = "\(id)-\(UUID().uuidString.lowercased())"
            let explicitHostname = (body.Hostname?.isEmpty == false) ? body.Hostname : nil

            // Handle networking configuration from request
            if let networkingConfig = body.NetworkingConfig,
                let endpointsConfig = networkingConfig.EndpointsConfig,
                !endpointsConfig.isEmpty
            {
                containerConfiguration.networks = endpointsConfig.map { (networkName, endpointSettings) in
                    // Use explicit Hostname, or first alias (compose service name), or default
                    let hostname = explicitHostname
                        ?? endpointSettings.Aliases?.first
                        ?? defaultHostname
                    return AttachmentConfiguration(network: networkName, options: AttachmentOptions(hostname: hostname))
                }
            } else if let networkingConfig = body.NetworkingConfig,
                let networks = networkingConfig.Networks,
                !networks.isEmpty
            {
                let hostname = explicitHostname ?? defaultHostname
                containerConfiguration.networks = networks.map { (networkName, _) in
                    return AttachmentConfiguration(network: networkName, options: AttachmentOptions(hostname: hostname))
                }
            } else if let hostConfig = body.HostConfig,
                let networkMode = hostConfig.NetworkMode,
                !networkMode.isEmpty
            {
                let hostname = explicitHostname ?? defaultHostname
                containerConfiguration.networks = [AttachmentConfiguration(network: networkMode, options: AttachmentOptions(hostname: hostname))]
            } else {
                let hostname = explicitHostname ?? defaultHostname
                containerConfiguration.networks = [AttachmentConfiguration(network: "default", options: AttachmentOptions(hostname: hostname))]
            }

            containerConfiguration.publishedPorts = publishedPorts

            // Handle DNS configuration from request
            let searchDomains = body.HostConfig?.DnsSearch ?? []
            let dnsOptions = body.HostConfig?.DnsOptions ?? []
            let domain = (body.Domainname?.isEmpty == false) ? body.Domainname : nil

            // Determine whether this container participates in DNS management.
            // Skip if: network is "none", or the container is itself a DNS container.
            let requestedLabels = body.Labels ?? [:]
            let isNetworkNone = containerConfiguration.networks.allSatisfy { $0.network == "none" }
            let isDnsContainer = requestedLabels["socktainer.role"] == "dns"
            var resolvedNameservers: [String] = body.HostConfig?.Dns ?? []

            if !isNetworkNone && !isDnsContainer,
                let dnsManager = req.application.storage[NetworkDNSManagerKey.self]
            {
                // Ensure a CoreDNS container exists for the first network and use its IP
                if let firstNetwork = containerConfiguration.networks.first {
                    do {
                        let dnsContainerIP = try await dnsManager.ensureDNSContainer(
                            networkId: firstNetwork.network)
                        resolvedNameservers = [dnsContainerIP]
                    } catch {
                        req.logger.warning(
                            "DNS container setup failed for network \(firstNetwork.network), falling back to default DNS: \(error)"
                        )
                    }
                }
            }

            // Always set DNS configuration to ensure /etc/resolv.conf is created
            containerConfiguration.dns = ContainerConfiguration.DNSConfiguration(
                nameservers: resolvedNameservers,
                domain: domain,
                searchDomains: searchDomains,
                options: dnsOptions
            )
            containerConfiguration.labels = requestedLabels

            var resolvedMounts: [Filesystem] = []

            // Process bind mounts from HostConfig.Binds
            var volumesOrFs: [VolumeOrFilesystem] = []
            if let binds = body.HostConfig?.Binds, !binds.isEmpty {
                volumesOrFs = try Parser.volumes(binds)
            }

            // Process mounts from HostConfig.Mounts
            var mountsOrFs: [VolumeOrFilesystem] = []
            if let mounts = body.HostConfig?.Mounts, !mounts.isEmpty {
                // Separate volume mounts from other mount types
                let volumeMounts = mounts.filter { $0.MountType.lowercased() == "volume" }
                let otherMounts = mounts.filter { $0.MountType.lowercased() != "volume" }

                // Handle volume mounts using the volume format (source:destination)
                if !volumeMounts.isEmpty {
                    let volumeStrings = volumeMounts.map { mount in
                        var volumeString = "\(mount.Source):\(mount.Target)"
                        if mount.ReadOnly == true {
                            volumeString += ":ro"
                        }
                        return volumeString
                    }
                    let volumeMountsOrFs = try Parser.volumes(volumeStrings)
                    mountsOrFs.append(contentsOf: volumeMountsOrFs)
                }

                // Handle other mount types (bind, tmpfs, etc.)
                if !otherMounts.isEmpty {
                    let mountStrings = otherMounts.map { mount in
                        var components: [String] = []

                        // Convert Docker mount type to Parser-supported type
                        let mountType = mount.MountType.lowercased() == "bind" ? "bind" : mount.MountType
                        components.append("type=\(mountType)")

                        // Add source if specified
                        if !mount.Source.isEmpty {
                            components.append("source=\(mount.Source)")
                        }

                        // Add destination/target
                        components.append("destination=\(mount.Target)")

                        // Add readonly flag if specified
                        if mount.ReadOnly == true {
                            components.append("ro")
                        }

                        return components.joined(separator: ",")
                    }
                    let otherMountsOrFs = try Parser.mounts(mountStrings)
                    mountsOrFs.append(contentsOf: otherMountsOrFs)
                }
            }

            // Resolve volumes from both volumes and mounts
            for item in (volumesOrFs + mountsOrFs) {
                switch item {
                case .filesystem(let fs):
                    resolvedMounts.append(fs)
                case .volume(let parsed):
                    // Check if volume exists by listing all volumes and finding a match
                    let existingVolumes = try await ClientVolume.list()
                    let existingVolume = existingVolumes.first { $0.name == parsed.name }

                    let volume: ContainerResource.Volume
                    if let existing = existingVolume {
                        // Volume exists, use it
                        volume = existing
                    } else {
                        // Volume doesn't exist, create it automatically (Docker behavior)
                        // might be revisited if https://github.com/apple/container/issues/690 is closed
                        req.logger.debug("Volume '\(parsed.name)' not found, creating it automatically")
                        volume = try await ClientVolume.create(
                            name: parsed.name,
                            driver: "local",
                            driverOpts: [:],
                            labels: [:]
                        )
                    }

                    let volumeMount = Filesystem.volume(
                        name: parsed.name,
                        format: volume.format,
                        source: volume.source,
                        destination: parsed.destination,
                        options: parsed.options
                    )
                    resolvedMounts.append(volumeMount)
                }
            }

            containerConfiguration.mounts = resolvedMounts

            // Apply memory limit: HostConfig.Memory > CLI --memory flag > 4 GiB fallback
            if let memory = body.HostConfig?.Memory, memory > 0 {
                containerConfiguration.resources.memoryInBytes = UInt64(memory)
            } else {
                let defaultMemory = req.application.storage[DefaultContainerMemoryKey.self] ?? (4 * 1024 * 1024 * 1024)
                containerConfiguration.resources.memoryInBytes = defaultMemory
            }

            req.logger.info("create: resolved \(resolvedMounts.count) mounts, creating container")
            let options = ContainerCreateOptions(autoRemove: body.HostConfig?.AutoRemove ?? false)
            let container: ContainerSnapshot
            do {
                let containerClient = ContainerClient()
                try await containerClient.create(configuration: containerConfiguration, options: options, kernel: kernel)
                container = try await containerClient.get(id: containerConfiguration.id)
                req.logger.debug("Container created successfully with ID: \(container.id)")
            } catch {
                req.logger.error("Failed to create container: \(error)")
                throw Abort(.internalServerError, reason: "Failed to create container: \(error)")
            }

            // Register container hostnames in the DNS table
            if !isNetworkNone && !isDnsContainer,
                let dnsServer = req.application.storage[SocktainerDNSServerKey.self]
            {
                let endpointAliases: [String: [String]]
                if let endpoints = body.NetworkingConfig?.EndpointsConfig {
                    endpointAliases = endpoints.compactMapValues { $0.Aliases }
                } else {
                    endpointAliases = [:]
                }
                for attachment in container.networks {
                    let ip = attachment.ipv4Address.address.description
                    dnsServer.register(hostname: attachment.hostname, ip: ip)
                    // Also register all aliases for this network
                    for alias in (endpointAliases[attachment.network] ?? []) {
                        dnsServer.register(hostname: alias, ip: ip)
                    }
                }
            }

            return RESTContainerCreate(
                Id: container.id,
                Warnings: []
            )
        }
    }
}
// Function to convert PortBindings from HostConfig to PublishedPorts
/*

    // handle PortBindings from HostConfig
    // example:
    //     "PortBindings":{
    //      "5432/tcp":[
    //         {
    //            "HostIp":"",
    //            "HostPort":""
    //         }
    //      ]
    //   },

    // needs to be converted to
    "publishedPorts": [
        {
          "hostAddress": "0.0.0.0",
          "containerPort": 5432,
          "hostPort": 5432,
          "proto": "tcp"
        }
      ],
*/
func convertPortBindings(from portBindings: [String: [PortBinding]]) throws -> [PublishPort] {
    var publishedPorts: [PublishPort] = []

    for (portSpec, bindings) in portBindings {
        // Parse the port specification (e.g., "5432/tcp")
        let components = portSpec.split(separator: "/")
        guard components.count == 2,
            let containerPort = UInt16(components[0])
        else {
            continue  // Skip invalid port specifications
        }

        let protoString = String(components[1])
        guard let proto = PublishProtocol(rawValue: protoString) else {
            continue  // Skip unsupported protocols
        }

        // Process each binding for this port
        for binding in bindings {
            // Use default values if not specified
            let hostAddress = binding.HostIp?.isEmpty == false ? binding.HostIp! : "0.0.0.0"

            // If HostPort is empty/nil, find an available port
            let hostPort: UInt16
            if let hostPortString = binding.HostPort, !hostPortString.isEmpty {
                if let parsedPort = UInt16(hostPortString) {
                    hostPort = parsedPort
                } else {
                    hostPort = UInt16(try findAvailablePort())
                }
            } else {
                hostPort = UInt16(try findAvailablePort())
            }

            let publishPort = PublishPort(
                hostAddress: try IPAddress(hostAddress),
                hostPort: hostPort,
                containerPort: containerPort,
                proto: proto,
                count: 1
            )

            publishedPorts.append(publishPort)
        }
    }

    return publishedPorts
}
