# System Architecture Specification

## 1. High-Level Architecture Diagram

```
[ Client Traffic (HTTP / HTTPS) ]
               │
               ▼
   [ Netty Event Loop Gateway ]
               │
               ▼
    [ Front-End Rate Limiter ]
               │ (Allowed)
               ▼
     [ In-Memory Cache (GET) ]
               │
               ├─(Hit)──► [ Return Cached Response ]
               │
               ▼ (Miss / Non-GET)
   [ Custom Load Balancer Engine ]
   (filters out unhealthy and circuit-breaker-OPEN nodes)
               │
               ▼ (WLC + Max Capacity / Weighted RR Overflow)
      [ Upstream Proxy Router ]
             │
             ├─────────────► [ Layer 7 HTTP Forwarding (WebClient / Netty) ]
             └─────────────► [ Layer 4 TCP Tunneling (SNI / Raw Sockets) ]
                              │
                              ▼
                   [ Backend Server Fleet ]
                              ▲
                              │ (Periodic Ping / 60s)
             [ Background Health-Check Worker ]
```

---

## 2. Component Breakdown & Data Flow

* **The Edge Layer (Netty & Spring WebFlux):** Handles asynchronous connection acceptance, thread management via event loops, and non-blocking I/O dispatching.
* **The Decision Plane (Load Balancer & State Registry):** A singleton bean maintaining thread-safe concurrent maps of backend nodes, atomic in-flight request counters, and smooth weighted round-robin state (per-node `currentWeight`) used for overflow.
* **The Resilience Plane (Rate Limiter & Circuit Breakers):** Acts as a protective barrier preventing cascading failures and overload situations. Open circuit breakers are filtered out during server selection.
* **The Observability & Control Plane:** Manages dynamic backend registration and exposes operational metrics via `/lb/metrics`.

## 3. Request Pipeline Order

1. Rate limiter
2. Cache lookup (hits return immediately and never touch backend accounting)
3. Server selection (health + breaker filter, then WLC with caps, then weighted RR overflow)
4. Proxy upstream (L7 forwarding, or L4 tunneling for HTTPS/TCP)
5. Decrement in-flight counter in `doFinally` on every exit path
