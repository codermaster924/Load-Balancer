# Business Logic Specification

## 1.
 The Custom Routing Algorithm (Weighted Least Connections + Max Capacity + Weighted Round Robin Overflow)

The load balancer maintains a thread-safe registry of `ServerNode` objects. Each node contains an `id`, `url`, `weight` (relative computational power), `maxCapacity` (hard cap on simultaneous in-flight requests, proportional to weight), `activeConnections` (`AtomicInteger`, counting in-flight requests), `isHealthy` (`AtomicBoolean`), and a circuit breaker state.

Design principle: equity, not equality. Every distribution decision, including overflow, must be proportional to weight. A weak server must never receive the same share as a strong one.

### Selection Algorithm Steps:
1. **Filter Eligible Pool:** Keep only servers where `isHealthy == true` AND the circuit breaker is not OPEN. If none remain, return HTTP `503 Service Unavailable`.
2. **Weighted Least Connections (WLC) Evaluation:**
   * Keep only servers where `activeConnections < maxCapacity`.
   * For each, compute: Score = activeConnections / weight.
   * Select the server with the lowest score. Break ties by highest weight.
   * Reserve the slot with a compare-and-set loop: re-check `activeConnections < maxCapacity` and increment as one atomic step. If the CAS fails, re-evaluate.
3. **Smooth Weighted Round Robin (WRR) Overflow Fallback:**
   * If ALL eligible servers have `activeConnections >= maxCapacity`, abandon WLC and use Smooth Weighted Round Robin (the nginx algorithm) across the eligible pool, so overflow is split proportionally to weight.
   * Per node keep `currentWeight` (starts at 0). On each overflow selection: add `weight` to every node's `currentWeight`, pick the node with the highest `currentWeight`, then subtract the sum of all weights from the chosen node's `currentWeight`.
   * This path is rare, so guard the state update with a lightweight lock or a single-writer structure.
   * Overflow selections still increment `activeConnections`, which may exceed `maxCapacity`.
4. **Connection Accounting:**
   * Atomically increment the chosen node's `activeConnections`.
   * **Mandatory Cleanup:** Guarantee the decrement in a reactive `doFinally()` or `finally` block on every exit path: success, error, timeout, and client cancellation.

---

## 2. Supporting Subsystems

### A. Front-End Rate Limiter
* Positioned at the entry point of the gateway pipeline.
* Implements a thread-safe Token Bucket algorithm.
* Dynamically adjusts its maximum refill rate based on the aggregate health score reported by the background health checker.

### B. In-Memory Caching Layer
* Implements Caffeine cache for idempotent requests (`GET` method).
* Cache key: method + host + URI + query parameters + values of headers listed in `Vary`.
* Do not cache requests carrying `Authorization` or responses carrying `Set-Cookie`. Respect `Cache-Control: no-store/private`.
* Coalesce simultaneous misses for the same key into a single upstream fetch.
* Intercepts requests prior to load balancer selection to return cached responses instantly. Cache hits must not touch backend connection accounting.

### C. Circuit Breakers
* Tracks rolling error rates per backend node.
* If error rates exceed a defined threshold within a sliding time window, the circuit trips open, immediately bypassing that node during selection until recovery timeout elapses, then moves to HALF-OPEN to probe with limited traffic.

### D. Periodic Health Checks
* A background `ScheduledExecutorService` executes every **60 seconds** (the circuit breaker provides faster passive failure detection between checks).
* Pings each backend's `/health` endpoint.
* Updates `isHealthy`, computes a rolling `healthScore`, and recalculates aggregate health metrics to feed into the adaptive rate limiter.
