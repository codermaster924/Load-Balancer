# API Contract & Technology Stack Specification

## 1. Technology Stack
* **Language & Runtime:** Java 17+ / Java 21
* **Framework:** Spring Boot 3.x (Reactive Stack / WebFlux using Netty)
* **Networking / Proxying:** Spring `WebClient` / Netty Channel Handlers
* **Concurrency & Utilities:** Java Concurrency API (`java.util.concurrent`), Caffeine Cache (for in-memory caching), Resilience4j concepts (implemented natively or via lightweight wrappers).

---

## 2. API Endpoints & Operational Routes

### A. Proxy & Traffic Routing Routes (Gateway Data Plane)
* **Route 1: Layer 7 HTTP Proxy**
  * **Path:** `/**` (Catch-all for HTTP methods: `GET`, `POST`, `PUT`, `DELETE`, etc.)
  * **Behavior:** Intercepts incoming HTTP traffic, executes the rate limiter, checks the cache (for `GET` requests), evaluates the custom load balancer algorithm, and proxies the request upstream.
* **Route 2: Layer 4 HTTPS / TCP Fallback Proxy**
  * **Path:** `/tunnel/**` or raw TCP port listener (or SNI-based routing filter)
  * **Behavior:** Bypasses deep L7 inspection and TLS termination, tunneling raw TCP bytes directly to the selected backend node.

### B. Gateway Management & Observability Endpoints (Control Plane)
* **Route 3: Get Backend Nodes Status & Metrics**
  * **Path:** `/lb/metrics`
  * **Method:** `GET`
  * **Response Payload (JSON):**
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
          "maxCapacity": 100,
          "activeConnections": 45,
          "isHealthy": true,
          "healthScore": 98.5
        }
      ]
    }
    ```
* **Route 4: Dynamic Backend Registration**
  * **Path:** `/lb/backends`
  * **Method:** `POST`
  * **Request Payload (JSON):**
    ```json
    {
      "id": "node-3",
      "url": "http://localhost:8083",
      "weight": 3,
      "maxCapacity": 80
    }
    ```
  * **Validation:** `weight` must be a positive integer. `maxCapacity` should be proportional to `weight` (`maxCapacity = weight x K`, where K is a configured constant). Either derive `maxCapacity` from `weight` when omitted, or reject registrations where the ratio deviates significantly from K, so the equity logic cannot be silently broken.
  * **Response:** HTTP `201 Created`
