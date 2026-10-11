Business Logic Specification
1. The Custom Routing Algorithm (Weighted Least Connections + Max Capacity + Weighted Round Robin Overflow)
The load balancer maintains a thread-safe registry of `ServerNode` objects. Each node contains an `id`, `url`, `weight` (relative computational power), `maxCapacity` (hard cap on simultaneous in-flight requests, proportional to weight), `activeConnections` (`AtomicInteger`, counting in-flight requests), `isHealthy` (`AtomicBoolean`), and a circuit breaker state.
Design principle: equity, not equality. Every distribution decision, including overflow, must be proportional to weight. A weak server must never receive the same share as a strong one.
Selection Algorithm Steps:
Filter Eligible Pool: Keep only servers where `isHealthy == true` AND the circuit breaker is not OPEN. If none remain, return HTTP `503 Service Unavailable`.
Weighted Least Connections (WLC) Evaluation:
Keep only servers where `activeConnections < maxCapacity`.
For each, compute: Score = activeConnections / weight.
Select the server with the lowest score. Break ties by highest weight.
Reserve the slot with a compare-and-set loop: re-check `activeConnections < maxCapacity` and increment as one atomic step. If the CAS fails, re-evaluate.
Smooth Weighted Round Robin (WRR) Overflow Fallback:
If ALL eligible servers have `activeConnections >= maxCapacity`, abandon WLC and use Smooth Weighted Round Robin (the nginx algorithm) across the eligible pool, so overflow is split proportionally to weight.
Per node keep `currentWeight` (starts at 0). On each overflow selection: add `weight` to every node's `currentWeight`, pick the node with the highest `currentWeight`, then subtract the sum of all weights from the chosen node's `currentWeight`.
This path is rare, so guard the state update with a lightweight lock or a single-writer structure.
Overflow selections still increment `activeConnections`, which may exceed `maxCapacity`.
Connection Accounting:
Atomically increment the chosen node's `activeConnections`.
Mandatory Cleanup: Guarantee the decrement in a reactive `doFinally()` or `finally` block on every exit path: success, error, timeout, and client cancellation.
---
2. Supporting Subsystems
A. Front-End Rate Limiter
Positioned at the entry point of the gateway pipeline.
Implements a thread-safe Token Bucket algorithm.
Dynamically adjusts its maximum refill rate based on the aggregate health score reported by the background health checker.
B. In-Memory Caching Layer
Implements Caffeine cache for idempotent requests (`GET` method).
Cache key: method + host + URI + query parameters + values of headers listed in `Vary`.
Do not cache requests carrying `Authorization` or responses carrying `Set-Cookie`. Respect `Cache-Control: no-store/private`.
Coalesce simultaneous misses for the same key into a single upstream fetch.
Intercepts requests prior to load balancer selection to return cached responses instantly. Cache hits must not touch backend connection accounting.
C. Circuit Breakers
Tracks rolling error rates per backend node.
If error rates exceed a defined threshold within a sliding time window, the circuit trips open, immediately bypassing that node during selection until recovery timeout elapses, then moves to HALF-OPEN to probe with limited traffic.
D. Periodic Health Checks
A background `ScheduledExecutorService` executes every 60 seconds (the circuit breaker provides faster passive failure detection between checks).
Pings each backend's `/health` endpoint.
Updates `isHealthy`, computes a rolling `healthScore`, and recalculates aggregate health metrics to feed into the adaptive rate limiter.
---
3. Agreed Operational Details
These decisions fill gaps the sections above leave open. They do not change the routing algorithm.
A. Circuit Breaker
Count-based window: the last `lb.circuit-breaker-window-size` (20) request outcomes per node.
The breaker is evaluated only after at least 10 outcomes are recorded.
Counted as errors: connect failures, timeouts, `5xx` responses. `4xx` is not an error.
OPEN lasts `lb.circuit-breaker-open-duration-ms`, then HALF_OPEN. HALF_OPEN admits a small number of probe requests through an atomic permit (`tryAcquireProbe`); a successful probe closes the breaker, a failed probe re-opens it.
B. Health Checks
A node passes when `/health` answers `2xx` within 2 seconds.
A node becomes unhealthy after 2 consecutive failures and healthy again after 2 consecutive passes.
A health check runs immediately when a node is registered, in addition to the 60 second schedule.
`healthScore` is an exponentially weighted moving average of check results (pass = 100, fail = 0).
C. Rate Limiter
One global token bucket (matches `globalRateLimitThreshold`).
Effective refill rate = `baseline x (average healthScore / 100)`, never below 10 % of the baseline.
Bucket capacity (burst) equals one second of baseline refill.
Rejected requests receive `429 Too Many Requests` with a `Retry-After` header.
D. Retries
At most `lb.max-retries` (2) retries per request, only for idempotent methods without a request body.
Retried only on connect failures or upstream `502`, `503`, `504`, and only before any response byte reaches the client.
Each retry re-runs selection with the failed node excluded.
A global retry budget (default 10 % of recent request volume) caps total retries so retries cannot amplify an outage.
E. Cache
Two-level lookup: a primary key (method + host + URI + query) stores the `Vary` header names seen on the response, and the full key adds those request header values.
Only `200` responses are cached. `Cache-Control: max-age` overrides the default TTL; `no-store` / `private` responses and `Set-Cookie` responses are never cached.
Entries larger than 1 MB are not cached.
F. Layer 4 Tunnel
A separate TCP listener (default port 8443); TLS is never terminated.
The SNI hostname is parsed from the ClientHello for logging and metrics only. There is no hostname-to-pool mapping.
Tunnels use the same selection engine and count toward the same `activeConnections` and `maxCapacity`.
The tunnel connects to the selected backend's host on the port of its registered URL.
G. Weighted Round Robin State
When overflow selection runs, `currentWeight` of every node outside the healthy, breaker-closed pool is reset to 0 so a returning node does not start with a stale balance. A per-request retry exclusion does not reset anything.