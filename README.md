# Custom Reactive L7 Load Balancer Gateway

A high-performance, non-blocking L7 Reverse Proxy and Load Balancer built from scratch with **Java 21**, **Spring Boot 3 (WebFlux)**, and **Reactor Netty**.

---

## 🚀 Key Features

### 1. Dynamic Control Plane
- **Zero-downtime Backend Registration**: Dynamically register upstream servers without restarting the proxy.
- **REST Control Endpoints**:
  - `POST /lb/backends` — Register a new upstream node (with initial health verification).
  - `DELETE /lb/backends/{id}` — Gracefully remove a node (in-flight connections drain safely).
  - `GET /lb/metrics` — Inspect live fleet health, circuit breaker statuses, and connection counters.

### 2. High-Performance L7 Reverse Proxy Data Plane
- **Fully Non-Blocking & Reactive**: Powered by Spring WebFlux and Reactor Netty for low latency and high concurrency.
- **Streaming Request/Response**: Memory-efficient streaming of request and response bodies.
- **Header Management**: Injects standard `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto` and strips hop-by-hop headers (`Connection`, `Keep-Alive`, `Transfer-Encoding`, etc.).

### 3. Resilience & Traffic Shaping
- **Adaptive Token Bucket Rate Limiting**: Token refill rate dynamically scales based on aggregate fleet health (EWMA score) to protect downstreams and prevent cascading failures.
- **Peak-EWMA & Weighted Least Connections**: Routes traffic to the backend with the best combination of weight, low active connection count, and low latency.
- **Per-Node Circuit Breakers**: Automatically opens circuits when consecutive failures or error thresholds are met, avoiding sickly backends.
- **Token-Bucket Retry Budget**: Retries idempotent requests (GET, HEAD, etc.) on alternative healthy nodes while strictly preventing retry storms.

### 4. Intelligent Caching
- **RFC-Compliant GET Response Cache**: Powered by **Caffeine** with configurable size and TTL.
- Respects `Cache-Control` (`no-store`, `private`, `max-age`), ignores responses setting `Set-Cookie`, and caps entry sizes to 1MB.

---

## 🏗️ Architecture & Request Pipeline

Every incoming HTTP request traverses the following pipeline order:

```mermaid
graph TD
    Client[Downstream Client] -->|HTTP Request| RateLimiter[1. Adaptive Rate Limiter]
    RateLimiter -->|Bucket Empty| R429[429 Too Many Requests]
    RateLimiter -->|Token Acquired| CacheCheck[2. Response Cache Lookup]
    CacheCheck -->|Cache Hit (GET)| CachedResponse[Return Cached 200]
    CacheCheck -->|Cache Miss| Selection[3. Selection Engine (Peak-EWMA / Least-Conn)]
    Selection -->|No Nodes Available| R503[503 Service Unavailable]
    Selection -->|Node Selected| Forward[4. Upstream WebClient]
    Forward -->|502/503/504 / Network Fail| RetryBudgetCheck{Can Retry & Budget Available?}
    RetryBudgetCheck -->|Yes| Selection
    RetryBudgetCheck -->|No| GatewayError[Error Response]
    Forward -->|Success 200 GET| CacheStore[Store in Caffeine Cache]
    CacheStore --> StreamDownstream[Stream to Downstream Client]
    Forward -->|Other Status| StreamDownstream
```

---

## ⚙️ Configuration (`application.yml`)

```yaml
load-balancer:
  capacity-per-weight-unit: 100
  connect-timeout-ms: 2000
  response-timeout-ms: 5000
  max-retries: 2
  retry-budget-fraction: 0.10
  rate-limit-baseline-rps: 10000
  cache-max-entries: 50000
  cache-default-ttl-seconds: 60
  health-check-interval-seconds: 10
  health-check-path: /health
```

---

## 📡 API Reference

### 1. Register a Backend Node
```bash
curl -X POST http://localhost:8080/lb/backends \
  -H "Content-Type: application/json" \
  -d '{
    "id": "srv-1",
    "url": "http://192.168.1.10:8081",
    "weight": 5,
    "maxCapacity": 500
  }'
```

### 2. Remove a Backend Node
```bash
curl -X DELETE http://localhost:8080/lb/backends/srv-1
```

### 3. Fleet Health & Metrics
```bash
curl http://localhost:8080/lb/metrics
```

**Response Example:**
```json
{
  "totalNodes": 2,
  "healthyNodes": 2,
  "aggregateHealthScore": 100.0,
  "nodes": [
    {
      "id": "srv-1",
      "url": "http://192.168.1.10:8081",
      "weight": 5,
      "activeConnections": 0,
      "healthy": true,
      "healthScore": 100.0,
      "circuitState": "CLOSED"
    }
  ]
}
```

---

## 🛠️ Build & Run

### Prerequisites
- Java 21+
- Maven Wrapper (included)

### Running Tests
```bash
./mvnw clean test
```

### Starting the Application
```bash
./mvnw spring-boot:run
```
The gateway will start on port `8080` (or the configured `server.port`).
