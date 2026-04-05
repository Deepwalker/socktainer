import ArgumentParser
import BuildInfo
import Foundation
import Vapor

struct DefaultContainerMemoryKey: StorageKey {
    typealias Value = UInt64
}

struct SocktainerDNSPortKey: StorageKey {
    typealias Value = Int
}

func parseMemoryString(_ s: String) -> UInt64 {
    let lowered = s.lowercased().trimmingCharacters(in: .whitespaces)
    let multipliers: [(String, UInt64)] = [
        ("g", 1024 * 1024 * 1024),
        ("gb", 1024 * 1024 * 1024),
        ("m", 1024 * 1024),
        ("mb", 1024 * 1024),
    ]
    for (suffix, mult) in multipliers {
        if lowered.hasSuffix(suffix), let n = UInt64(lowered.dropLast(suffix.count)) {
            return n * mult
        }
    }
    // Assume bytes if just a number
    return UInt64(lowered) ?? (4 * 1024 * 1024 * 1024)
}

// CLI options
struct CLIOptions: ParsableArguments {
    @ArgumentParser.Flag(name: .long, help: "Show version")
    var version: Bool = false

    @ArgumentParser.Flag(name: .long, inversion: .prefixedNo, help: "Check Apple Container compatibility and exit")
    var checkCompatibility: Bool = true

    @ArgumentParser.Option(name: .long, help: "Default container memory limit (e.g. 4g, 2048m). Default: 4g")
    var memory: String = "4g"

    @ArgumentParser.Option(name: .long, help: "UDP port for the container DNS server. Default: 2054")
    var dnsPort: Int = 2054
}

// Parse CLI before starting the app
let options = CLIOptions.parseOrExit()

if options.version {
    print("socktainer: \(getBuildVersion()) (git commit: \(getBuildGitCommit()))")
    exit(0)
}

if options.checkCompatibility {
    await AppleContainerVersionCheck.performCompatibilityCheck()
}

// Ignore real CLI args for Vapor: always behave like `socktainer serve`
let executable = CommandLine.arguments.first ?? "socktainer"
let vaporArgs = [executable, "serve"]

// Detect environment and set up logging
var env = try Environment.detect(arguments: vaporArgs)
try LoggingSystem.bootstrap(from: &env)

// Parse memory limit
let defaultMemoryBytes = parseMemoryString(options.memory)

// Create and configure the Vapor application
let app = try await Application.make(env)
app.storage[DefaultContainerMemoryKey.self] = defaultMemoryBytes
app.storage[SocktainerDNSPortKey.self] = options.dnsPort
try prepareUnixSocket(for: app, homeDirectory: ProcessInfo.processInfo.environment["HOME"])
try await configure(app)

// Start the app
try await app.execute()
