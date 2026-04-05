import CryptoKit
import Foundation
import Logging

private let log = Logger(label: "socktainer.context")

/// Creates (or updates) the "socktainer" Docker context pointing at our Unix socket,
/// and sets it as the current context if the current one is "default" or already "socktainer".
func setupDockerContext(socketPath: String, homeDirectory: String) {
    let contextName = "socktainer"
    let hashHex = SHA256.hash(data: Data(contextName.utf8))
        .map { String(format: "%02x", $0) }.joined()

    let metaDir = "\(homeDirectory)/.docker/contexts/meta/\(hashHex)"
    let metaPath = "\(metaDir)/meta.json"

    let meta: [String: Any] = [
        "Name": contextName,
        "Metadata": [:] as [String: String],
        "Endpoints": [
            "docker": [
                "Host": "unix://\(socketPath)",
                "SkipTLSVerify": false,
            ]
        ],
    ]

    do {
        try FileManager.default.createDirectory(
            atPath: metaDir, withIntermediateDirectories: true)
        let data = try JSONSerialization.data(
            withJSONObject: meta, options: [.sortedKeys])
        try data.write(to: URL(fileURLWithPath: metaPath))
        log.info("[context] wrote Docker context '\(contextName)' → \(socketPath)")
    } catch {
        log.warning("[context] failed to write Docker context: \(error)")
        return
    }

    // Set as current context if it's "default" or already "socktainer"
    let configPath = "\(homeDirectory)/.docker/config.json"
    do {
        var config: [String: Any]
        if let data = FileManager.default.contents(atPath: configPath),
            let parsed = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        {
            config = parsed
        } else {
            config = [:]
        }

        let current = config["currentContext"] as? String ?? "default"
        guard current == "default" || current == contextName else {
            log.info("[context] current context is '\(current)', not switching")
            return
        }

        config["currentContext"] = contextName
        let updated = try JSONSerialization.data(
            withJSONObject: config, options: [.prettyPrinted, .sortedKeys])
        try updated.write(to: URL(fileURLWithPath: configPath))
        log.info("[context] set current Docker context to '\(contextName)'")
    } catch {
        log.warning("[context] failed to update Docker config.json: \(error)")
    }
}
