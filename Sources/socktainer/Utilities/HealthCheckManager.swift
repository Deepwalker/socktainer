import ContainerAPIClient
import Foundation
import Logging
import Vapor

struct HealthCheckManagerKey: StorageKey {
    typealias Value = HealthCheckManager
}

/// Runs Docker healthchecks inside containers and tracks their status.
///
/// Apple Container has no native healthcheck support. We implement it here by
/// periodically exec-ing the test command inside the container and tracking
/// the result so that `GET /containers/{id}/json` returns a real health status.
actor HealthCheckManager {
    private var statuses: [String: ContainerHealth] = [:]
    private var tasks: [String: Task<Void, Never>] = [:]
    private let log = Logger(label: "socktainer.healthcheck")

    func currentHealth(for id: String) -> ContainerHealth? {
        statuses[id]
    }

    /// Start running healthchecks for a container. No-op if already running.
    func start(containerId: String, config: HealthcheckConfig) {
        guard tasks[containerId] == nil else { return }
        statuses[containerId] = ContainerHealth(Status: "starting", FailingStreak: 0, Log: [])
        let task = Task { [self] in
            await self.runLoop(containerId: containerId, config: config)
        }
        tasks[containerId] = task
        log.info("[healthcheck] started for \(containerId)")
    }

    /// Stop the healthcheck loop for a container.
    func stop(containerId: String) {
        tasks.removeValue(forKey: containerId)?.cancel()
        statuses.removeValue(forKey: containerId)
    }

    // Called from runLoop to update status while still on actor
    private func updateStatus(id: String, health: ContainerHealth) {
        guard tasks[id] != nil else { return }  // already stopped
        statuses[id] = health
    }

    private func isActive(id: String) -> Bool {
        tasks[id] != nil
    }

    // MARK: - Private

    private func runLoop(containerId: String, config: HealthcheckConfig) async {
        // Intervals are stored as nanoseconds in the healthcheck config
        let startPeriodNs = UInt64(max((config.StartPeriod ?? 0), 0))
        let intervalNs = UInt64(max((config.Interval ?? 30_000_000_000), 1_000_000_000))
        let timeoutNs = UInt64(max((config.Timeout ?? 30_000_000_000), 1_000_000_000))
        let maxRetries = config.Retries ?? 3

        if startPeriodNs > 0 {
            try? await Task.sleep(nanoseconds: startPeriodNs)
        }

        var failingStreak = 0

        while !Task.isCancelled {
            guard await isActive(id: containerId) else { return }

            let exitCode = await runCheck(containerId: containerId, config: config, timeoutNs: timeoutNs)

            guard !Task.isCancelled else { return }

            if exitCode == 0 {
                failingStreak = 0
                await updateStatus(id: containerId, health: ContainerHealth(Status: "healthy", FailingStreak: 0, Log: []))
                log.debug("[healthcheck] \(containerId) → healthy")
            } else {
                failingStreak += 1
                let status = failingStreak >= maxRetries ? "unhealthy" : "starting"
                await updateStatus(id: containerId, health: ContainerHealth(Status: status, FailingStreak: failingStreak, Log: []))
                log.info("[healthcheck] \(containerId) → \(status) (streak=\(failingStreak), exit=\(exitCode))")
            }

            try? await Task.sleep(nanoseconds: intervalNs)
        }
    }

    private func runCheck(containerId: String, config: HealthcheckConfig, timeoutNs: UInt64) async -> Int32 {
        guard let test = config.Test, !test.isEmpty else { return 0 }

        let cmd: [String]
        switch test.first {
        case "CMD-SHELL":
            cmd = ["/bin/sh", "-c", test.dropFirst().joined(separator: " ")]
        case "CMD":
            cmd = Array(test.dropFirst())
        default:
            cmd = test
        }
        guard !cmd.isEmpty else { return 0 }

        do {
            let containerClient = ContainerClient()
            guard let container = try? await containerClient.get(id: containerId) else { return 1 }

            var processConfig = container.configuration.initProcess
            processConfig.executable = cmd[0]
            processConfig.arguments = Array(cmd.dropFirst())
            processConfig.terminal = false
            processConfig.user = .id(uid: 0, gid: 0)

            let processId = "hc-\(UUID().uuidString.lowercased())"
            let process = try await containerClient.createProcess(
                containerId: containerId,
                processId: processId,
                configuration: processConfig,
                stdio: [nil, nil, nil]
            )
            try await process.start()

            return try await withThrowingTaskGroup(of: Int32.self) { group in
                group.addTask { try await process.wait() }
                group.addTask {
                    try await Task.sleep(nanoseconds: timeoutNs)
                    return 1
                }
                let result = try await group.next() ?? 1
                group.cancelAll()
                return result
            }
        } catch {
            log.debug("[healthcheck] \(containerId) exec error: \(error)")
            return 1
        }
    }
}
