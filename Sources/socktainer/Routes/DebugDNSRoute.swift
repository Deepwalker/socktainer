import Vapor

struct DebugDNSRoute: RouteCollection {
    func boot(routes: RoutesBuilder) throws {
        routes.get("_socktainer", "dns") { req -> Response in
            guard let dnsServer = req.application.storage[SocktainerDNSServerKey.self] else {
                throw Abort(.serviceUnavailable, reason: "DNS server not running")
            }
            let entries = dnsServer.listEntries()
            let body = try JSONEncoder().encode(entries)
            return Response(
                status: .ok,
                headers: ["Content-Type": "application/json"],
                body: .init(data: body)
            )
        }
    }
}
