# Project Master Briefing & IDE Instructions: Custom L7/L4 Load Balancer & Edge Gateway

## 1. Overview & Objectives
You are building a high-performance, production-grade custom Layer 7 and Layer 4 Hybrid Load Balancer and Proxy Gateway in **Java Spring Boot 3.x (WebFlux / Netty)**. This project is specifically engineered for a resume targeting cloud security and proxy infrastructure companies (such as Zscaler).

**Core Directive:** You must write the core components (routing engine, custom load-balancing algorithm, adaptive rate limiter, circuit breakers, and health checker) **yourself** using standard Java and Netty primitives. Do not use pre-built load balancer libraries (e.g., Spring Cloud LoadBalancer, Ribbon, etc.).

**Design principle:** Equity, not equality. All distribution decisions, including overflow, are proportional to server weight.

---

## 2. Structure of Supporting Documentation Files
To guide your coding process, refer to the following companion files located in the repository root:
* **`api_contract_tech_stack.md`**: Defines all required REST endpoints, operational routes, request/response payloads, and the underlying technology stack.
* **`business_logic_specification.md`**: Details the granular implementation rules for the custom Weighted Least Connections algorithm, max capacity constraints, Weighted Round Robin overflow, health checks, caching, and rate limiting.
* **`system_architecture.md`**: Outlines the system components, data flow pipelines, threading model, and network socket handling across Layer 7 (HTTP) and Layer 4 (HTTPS/TCP).

---

## 3. Implementation Guidelines for the IDE
When writing code for this project, adhere strictly to these engineering constraints:
1. **Concurrency & Thread Safety:** Use `ConcurrentHashMap`, `AtomicInteger`, `AtomicLong`, and lock-free thread-safe constructs across all shared state registries (backend node states, in-flight request counters, weighted round-robin state). The cap check and counter increment must be one atomic compare-and-set step.
2. **Asynchronous Non-Blocking I/O:** Leverage Spring WebFlux and Netty event loops. Do not block threads during proxy forwarding or health checking.
3. **Resource Cleanup:** Every connection incremented in the load balancer router **must** be guaranteed a corresponding decrement in a reactive `doFinally` or `finally` block on every exit path (success, error, timeout, client cancellation) to prevent counter leaks.
4. **Timeouts:** Configure connect, response, and total request timeouts so slow clients and backends cannot hold capacity slots indefinitely.
5. **Retries:** Only retry idempotent requests on connect failures or clear 5xx, and cap retries with a budget.
