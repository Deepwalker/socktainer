import Vapor

struct EventBroadcasterKey: StorageKey {
    typealias Value = EventBroadcaster
}

/// Dynamic attributes map — includes standard fields + container labels.
/// Encodes as a flat JSON object: {"containerExitCode":"","image":"","name":"...", "label.key":"value", ...}
struct ActorAttributes: Codable {
    private var storage: [String: String]

    init(name: String, image: String = "", containerExitCode: String = "", labels: [String: String] = [:]) {
        var s = [String: String]()
        s["containerExitCode"] = containerExitCode
        s["image"] = image
        s["name"] = name
        for (k, v) in labels {
            s[k] = v
        }
        self.storage = s
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        self.storage = try container.decode([String: String].self)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(storage)
    }
}

struct DockerActor: Codable {
    let ID: String
    let Attributes: ActorAttributes
}

struct DockerEvent: Codable {
    let status: String
    let id: String
    let from: String
    let `Type`: String
    let Action: String
    let Actor: DockerActor
    let scope: String
    let time: Int
    let timeNano: UInt64
}

extension DockerEvent {
    static func simpleEvent(id: String, type: String, status: String, image: String = "", labels: [String: String] = [:]) -> DockerEvent {
        let now = Date()
        let timeSeconds = Int(now.timeIntervalSince1970)
        let timeNano = UInt64(now.timeIntervalSince1970 * 1_000_000_000)

        let actorAttributes = ActorAttributes(
            name: id,
            image: image,
            labels: labels
        )
        let actor = DockerActor(
            ID: id,
            Attributes: actorAttributes
        )

        return DockerEvent(
            status: status,
            id: id,
            from: image,
            Type: type,
            Action: status,
            Actor: actor,
            scope: "local",
            time: timeSeconds,
            timeNano: timeNano
        )
    }
}

actor EventBroadcaster {
    private var continuations: [UUID: AsyncStream<DockerEvent>.Continuation] = [:]

    func stream() -> AsyncStream<DockerEvent> {
        let id = UUID()

        return AsyncStream { continuation in
            // Safely register continuation inside the actor
            Task {
                self.addContinuation(id: id, continuation)
            }

            // Handle termination safely via actor
            continuation.onTermination = { @Sendable _ in
                Task {
                    await self.removeContinuation(id: id)
                }
            }
        }
    }

    func broadcast(_ event: DockerEvent) {
        for continuation in continuations.values {
            continuation.yield(event)
        }
    }

    private func addContinuation(id: UUID, _ continuation: AsyncStream<DockerEvent>.Continuation) {
        continuations[id] = continuation
    }

    private func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }
}
