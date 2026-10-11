API Contract & Technology Stack Specification
1. Technology Stack
Language & Runtime: Java 17+ / Java 21
Framework: Spring Boot 3.x (Reactive Stack / WebFlux using Netty)
Networking / Proxying: Spring `WebClient` / Netty Channel Handlers
Concurrency & Utilities: Java Concurrency API (`java.util.concurrent`), Caffeine Cache (for in-memory caching), Resilience4j concepts (implemented natively or via lightweight wrappers).
---
2. API Endpoints & Operational Routes
A. Proxy & Traffic Routing Routes (Gateway Data Plane)
Route 1: Layer 7 HTTP Proxy
Path: `/**` (Catch-all for HTTP methods: `GET`, `POST`, `PUT`, `DELETE`, etc.). `/lb/**` and `/actuator/**` are reserved for the control plane and are never proxied.
Behavior: Intercepts incoming HTTP traffic, executes the rate limiter, checks the cache (for `GET` requests), evaluates the custom load balancer algorithm, and proxies the request upstream.
Route 2: Layer 4 HTTPS / TCP Fallback Proxy
Path: a dedicated raw TCP listener (`lb.tunnel-port`, default 8443). An HTTP path cannot carry raw TLS bytes, so there is no `/tunnel/**` route.
Behavior: Bypasses deep L7 inspection and TLS termination, tunneling raw TCP bytes directly to the selected backend node.
B. Gateway Management & Observability Endpoints (Control Plane)
Route 3: Get Backend Nodes Status & Metrics
Path: `/lb/metrics`
Method: `GET`
Response Payload (JSON):
    ```json
    {
      "totalBackends": 3,
      "healthyBackends": 2,
      "globalRateLimitThreshold": 1000,
      "nodes": [
        {
          "id": "node-1",
          "url": "http://localhost:8081",
          "weight": 5,
          "maxCapacity": 500,
          "activeConnections": 45,
          "isHealthy": true,
          "healthScore": 98.5
        }
      ]
    }
    ```
Route 4: Dynamic Backend Registration
Path: `/lb/backends`
Method: `POST`
Request Payload (JSON):
    ```json
    {
      "id": "node-3",
      "url": "http://localhost:8083",
      "weight": 3,
      "maxCapacity": 300
    }
    ```
Validation: `weight` must be a positive integer. `maxCapacity` should be proportional to `weight` (`maxCapacity = weight x K`, where K is the configured constant `lb.capacity-per-weight-unit`, default 100; the examples above use that default). Either derive `maxCapacity` from `weight` when omitted, or reject registrations where the ratio deviates significantly from K, so the equity logic cannot be silently broken.
Response: HTTP `201 Created`. Registering an existing `id` replaces that node and resets its counters.
Route 5: Backend Removal (graceful drain)
Path: `/lb/backends/{id}`
Method: `DELETE`
Behavior: Stops routing new requests to the node immediately; requests already in flight are allowed to finish. The node is removed from the registry once its in-flight count reaches zero.
Response: HTTP `202 Accepted` (or `404` if the id is unknown)