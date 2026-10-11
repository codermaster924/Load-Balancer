package com.loadbalancer.control;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.health.HealthChecker;
import com.loadbalancer.resilience.CircuitBreakerRegistry;
import com.loadbalancer.routing.BackendRegistry;
import com.loadbalancer.routing.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reactive HTTP handler for the Load Balancer control plane.
 *
 * <p>Implements the management routes from specification Section 2.B:
 * <ul>
 *   <li>{@code POST   /lb/backends}     — register a new backend node (Route 4)</li>
 *   <li>{@code DELETE /lb/backends/{id}} — gracefully drain and remove a node (Route 5)</li>
 *   <li>{@code GET    /lb/metrics}       — fleet status and observability metrics (Route 3)</li>
 * </ul>
 * </p>
 */
@Component
public class ControlPlaneHandler {

    private static final Logger log = LoggerFactory.getLogger(ControlPlaneHandler.class);

    private final BackendRegistry backendRegistry;
    private final HealthChecker healthChecker;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final LoadBalancerProperties props;

    public ControlPlaneHandler(BackendRegistry backendRegistry,
                               HealthChecker healthChecker,
                               CircuitBreakerRegistry circuitBreakerRegistry,
                               LoadBalancerProperties props) {
        this.backendRegistry       = backendRegistry;
        this.healthChecker         = healthChecker;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.props                 = props;
    }

    // ── POST /lb/backends ─────────────────────────────────────────────────

    /**
     * Registers a new backend node.
     *
     * <p>On success:
     * <ul>
     *   <li>Node is added to {@link BackendRegistry} (replacing any existing node with the same id).</li>
     *   <li>An immediate async health check is fired so the node's {@code isHealthy} state
     *       and {@code healthScore} are populated before the next scheduled cycle.</li>
     *   <li>Returns {@code 201 Created} with the node details.</li>
     * </ul>
     * </p>
     */
    public Mono<ServerResponse> registerBackend(ServerRequest request) {
        return request.bodyToMono(BackendRegistrationRequest.class)
                .flatMap(req -> {
                    // Field-presence validation
                    if (!req.isValid()) {
                        return badRequest("id, url, and weight (> 0) are required");
                    }

                    ServerNode node;
                    try {
                        node = backendRegistry.register(
                                req.getId(), req.getUrl(), req.getWeight(), req.getMaxCapacity());
                    } catch (IllegalArgumentException e) {
                        return badRequest(e.getMessage());
                    }

                    // Immediate health check — fire-and-forget; does not block the 201 response
                    healthChecker.checkNode(node)
                            .subscribe(
                                    null,
                                    err -> log.warn("Immediate health check failed for node '{}': {}",
                                            node.getId(), err.getMessage())
                            );

                    log.info("Control plane: registered backend node '{}'", node.getId());

                    Map<String, Object> body = nodeToMap(node);
                    return ServerResponse.status(HttpStatus.CREATED)
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(body);
                })
                .onErrorResume(e -> {
                    log.error("Error registering backend: {}", e.getMessage());
                    return ServerResponse.badRequest()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(Map.of("error", "Invalid request body: " + e.getMessage()));
                });
    }

    // ── DELETE /lb/backends/{id} ──────────────────────────────────────────

    /**
     * Gracefully removes a backend node.
     *
     * <p>Per spec Route 5: new requests to the node stop immediately (it is removed
     * from the registry so the selection engine cannot pick it). Any in-flight requests
     * that were already dispatched continue until their {@code doFinally} hook fires — the
     * selection engine's CAS reservation guarantees the counter was already incremented,
     * so those requests complete naturally.</p>
     *
     * <p>Returns {@code 202 Accepted} on success, {@code 404} if the id is unknown.</p>
     */
    public Mono<ServerResponse> removeBackend(ServerRequest request) {
        String id = request.pathVariable("id");

        return backendRegistry.findById(id)
                .map(node -> {
                    backendRegistry.remove(id);
                    log.info("Control plane: removed backend node '{}' (was {})",
                            id, node.isHealthy() ? "healthy" : "unhealthy");

                    Map<String, Object> body = Map.of(
                            "message", "Node '" + id + "' removed; in-flight requests will complete.",
                            "activeConnectionsAtRemoval", node.getActiveConnections()
                    );
                    return (Mono<ServerResponse>) ServerResponse.accepted()
                            .contentType(MediaType.APPLICATION_JSON)
                            .bodyValue(body);
                })
                .orElseGet(() -> ServerResponse.status(HttpStatus.NOT_FOUND)
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(Map.of("error", "No backend node found with id: " + id))
                );
    }

    // ── GET /lb/metrics ───────────────────────────────────────────────────

    /**
     * Returns a live fleet snapshot.
     *
     * <p>Response schema (spec Route 3):
     * <pre>{@code
     * {
     *   "totalBackends": 3,
     *   "healthyBackends": 2,
     *   "aggregateHealthScore": 91.4,
     *   "globalRateLimitThreshold": 1000,
     *   "nodes": [
     *     {
     *       "id": "node-1",
     *       "url": "http://localhost:8081",
     *       "weight": 5,
     *       "maxCapacity": 500,
     *       "activeConnections": 45,
     *       "isHealthy": true,
     *       "healthScore": 98.5,
     *       "circuitBreakerState": "CLOSED"
     *     }
     *   ]
     * }
     * }</pre></p>
     */
    public Mono<ServerResponse> getMetrics(ServerRequest request) {
        Collection<ServerNode> nodes = backendRegistry.allNodes();

        List<Map<String, Object>> nodeList = nodes.stream()
                .map(this::nodeToDetailedMap)
                .collect(Collectors.toList());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("totalBackends",            backendRegistry.size());
        response.put("healthyBackends",          backendRegistry.healthyCount());
        response.put("aggregateHealthScore",     Math.round(healthChecker.getAggregateHealthScore() * 10.0) / 10.0);
        response.put("globalRateLimitThreshold", props.getRateLimitBaselineRps());
        response.put("nodes",                    nodeList);

        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(response);
    }

    // ── Private helpers ───────────────────────────────────────────────────

    /** Minimal node representation used in the 201 Created response. */
    private Map<String, Object> nodeToMap(ServerNode node) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id",                node.getId());
        m.put("url",               node.getUrl());
        m.put("weight",            node.getWeight());
        m.put("maxCapacity",       node.getMaxCapacity());
        m.put("activeConnections", node.getActiveConnections());
        m.put("isHealthy",         node.isHealthy());
        m.put("healthScore",       node.getHealthScore());
        return m;
    }

    /** Full node representation including circuit breaker state for /lb/metrics. */
    private Map<String, Object> nodeToDetailedMap(ServerNode node) {
        Map<String, Object> m = nodeToMap(node);
        m.put("circuitBreakerState", circuitBreakerRegistry.getState(node.getId()).name());
        return m;
    }

    private Mono<ServerResponse> badRequest(String message) {
        return ServerResponse.badRequest()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of("error", message));
    }
}
