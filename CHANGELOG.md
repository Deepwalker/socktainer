# Change Log

Notable changes.

## April 2026

### [unreleased]
- Container-to-container DNS: internal UDP DNS server plus per-network CoreDNS container, with query logging and a `GET /_socktainer/dns` debug endpoint. (`--dns-port`, default `2054`)
- Healthcheck support: run the container's `HEALTHCHECK` test inside the container via `createProcess`, track `starting → healthy / unhealthy`, expose it on inspect — unblocks Docker Compose `depends_on: condition: service_healthy`.
- Auto-create Docker context `socktainer` on startup pointing at the Unix socket; switches current context only if it was `default` or `socktainer`.
- Auto-fallback to `linux/amd64` (Rosetta) when pulling or running an image with no `arm64` variant on arm64 hosts.
- Mount named volumes with `nosync` to match colima dev-environment behavior on write-heavy workloads (postgres WAL, Kafka).
- Use bsdtar auto-detection for build context compression, handling gzip payloads sent with `Content-Type: application/x-tar` (Docker Compose).
- Use first `EndpointsConfig.Aliases` entry as container hostname so Compose containers get service names (e.g. `postgres`) instead of generated IDs.
- Reject ambiguous short container ID prefix lookups instead of silently returning the first match.
- Close unconsumed pipe fds and resume hanging continuations in `AttachManager.cleanup()` to prevent fd/task leaks when `/start` is never called.
- Configurable container memory via CLI option.
