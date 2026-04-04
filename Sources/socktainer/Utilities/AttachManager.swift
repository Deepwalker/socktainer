import ContainerAPIClient
import Foundation

/// Coordinates the docker run flow between /attach and /start.
///
/// Flow:
/// 1. /attach creates pipes, dup's fds, stores write fds here, returns streaming response
/// 2. /start retrieves write fds, passes them to bootstrap(), provides process back
/// 3. /attach's streaming tasks read from dup'd read handles, await process for lifecycle
actor AttachManager {
    static let shared = AttachManager()

    struct PendingAttach: Sendable {
        /// Dup'd pipe write fd for stdout (passed to bootstrap by /start). -1 if not attached.
        let stdoutWriteFd: Int32
        /// Dup'd pipe write fd for stderr (passed to bootstrap by /start). -1 if not attached.
        let stderrWriteFd: Int32
    }

    private var pendingAttaches: [String: PendingAttach] = [:]
    private var processes: [String: any ClientProcess] = [:]
    private var processContinuations: [String: [CheckedContinuation<(any ClientProcess)?, Never>]] = [:]

    /// Called by /attach to store pipe write fds for /start to consume.
    func storePipes(id: String, attach: PendingAttach) {
        pendingAttaches[id] = attach
    }

    /// Called by /start to retrieve and consume pipe write fds for bootstrap.
    func consumePipes(id: String) -> PendingAttach? {
        pendingAttaches.removeValue(forKey: id)
    }

    /// Called by /start after bootstrap to provide the process to /attach's streaming tasks.
    func setProcess(id: String, process: any ClientProcess) {
        processes[id] = process
        if let continuations = processContinuations.removeValue(forKey: id) {
            for c in continuations {
                c.resume(returning: process)
            }
        }
    }

    /// Called by /attach's streaming tasks to wait for /start to provide the process.
    func awaitProcess(id: String) async -> (any ClientProcess)? {
        if let p = processes[id] { return p }
        return await withCheckedContinuation { c in
            processContinuations[id, default: []].append(c)
        }
    }

    /// Clean up all state for a container.
    func cleanup(id: String) {
        pendingAttaches.removeValue(forKey: id)
        processes.removeValue(forKey: id)
    }
}
