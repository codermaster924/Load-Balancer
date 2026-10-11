package com.loadbalancer.health;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.routing.BackendRegistry;
import com.loadbalancer.routing.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Background health checker and EWMA health score evaluator.
 *
 * <p>Implements specification Section 3.B:
 * <ul>
 *   <li>Runs every 60 seconds against each backend's {@code /health} endpoint.</li>
 *   <li>Runs an immediate check when a node is registered.</li>
 *   <li>Pass condition: HTTP 2xx response within 2 seconds.</li>
 *   <li>Flapping protection: node becomes unhealthy after 2 consecutive failures;
 *       becomes healthy again after 2 consecutive passes.</li>
 *   <li>Computes an Exponentially Weighted Moving Average (EWMA) {@code healthScore}
 *       (pass = 100, fail = 0) per node and aggregate fleet health score.</li>
 * </ul>
 * </p>
 */
@Component
public class HealthChecker {

    private static final Logger log = LoggerFactory.getLogger(HealthChecker.class);

    private static final Duration HEALTH_TIMEOUT = Duration.ofSeconds(2);
    private static final double EWMA_ALPHA = 0.25; // smoothing factor (25% weight to newest sample)

    private final BackendRegistry backendRegistry;
    private final WebClient webClient;

    /** Node ID -> consecutive check counters */
    private final ConcurrentHashMap<String, AtomicInteger> consecutivePasses = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> consecutiveFailures = new ConcurrentHashMap<>();

    public HealthChecker(BackendRegistry backendRegistry, WebClient webClient) {
        this.backendRegistry = backendRegistry;
        this.webClient = webClient;
    }

    /**
     * Periodic scheduled check running every 60 seconds across all registered backends.
     */
    @Scheduled(fixedRate = 60000, initialDelay = 60000)
    public void runPeriodicCheck() {
        checkAllNodes().subscribe(
                null,
                error -> log.error("Error during periodic health check cycle", error)
        );
    }

    /**
     * Pings all registered nodes concurrently without blocking.
     */
    public Mono<Void> checkAllNodes() {
        Collection<ServerNode> nodes = backendRegistry.allNodes();
        if (nodes.isEmpty()) {
            return Mono.empty();
        }

        return Flux.fromIterable(nodes)
                .flatMap(this::checkNode)
                .then();
    }

    /**
     * Pings a single node's {@code /health} endpoint and updates state accordingly.
     */
    public Mono<Void> checkNode(ServerNode node) {
        String healthUrl = buildHealthUrl(node);

        return webClient.get()
                .uri(healthUrl)
                .exchangeToMono(response -> {
                    boolean pass = response.statusCode().is2xxSuccessful();
                    return response.releaseBody().thenReturn(pass);
                })
                .timeout(HEALTH_TIMEOUT)
                .onErrorReturn(false)
                .doOnNext(passed -> updateNodeHealth(node, passed))
                .then();
    }

    private void updateNodeHealth(ServerNode node, boolean passed) {
        String id = node.getId();
        AtomicInteger passes = consecutivePasses.computeIfAbsent(id, k -> new AtomicInteger(0));
        AtomicInteger failures = consecutiveFailures.computeIfAbsent(id, k -> new AtomicInteger(0));

        // Update EWMA health score (pass = 100.0, fail = 0.0)
        double outcomeValue = passed ? 100.0 : 0.0;
        double currentScore = node.getHealthScore();
        double newScore = (EWMA_ALPHA * outcomeValue) + ((1.0 - EWMA_ALPHA) * currentScore);
        node.setHealthScore(Math.round(newScore * 10.0) / 10.0);

        if (passed) {
            failures.set(0);
            int consecutive = passes.incrementAndGet();
            if (consecutive >= 2 && !node.isHealthy()) {
                node.setHealthy(true);
                log.info("Backend node '{}' recovered to HEALTHY after 2 consecutive passes (score={})",
                        id, node.getHealthScore());
            }
        } else {
            passes.set(0);
            int consecutive = failures.incrementAndGet();
            if (consecutive >= 2 && node.isHealthy()) {
                node.setHealthy(false);
                log.warn("Backend node '{}' marked UNHEALTHY after 2 consecutive failures (score={})",
                        id, node.getHealthScore());
            }
        }
    }

    /**
     * Calculates the aggregate health score across all registered nodes in [0.0, 100.0].
     * If no nodes are registered, defaults to 100.0.
     */
    public double getAggregateHealthScore() {
        Collection<ServerNode> nodes = backendRegistry.allNodes();
        if (nodes.isEmpty()) {
            return 100.0;
        }

        double totalScore = 0.0;
        for (ServerNode node : nodes) {
            totalScore += node.getHealthScore();
        }
        return totalScore / nodes.size();
    }

    private String buildHealthUrl(ServerNode node) {
        String base = node.getUrl();
        if (base.endsWith("/")) {
            return base + "health";
        }
        return base + "/health";
    }

    // Testing helpers
    public int getConsecutivePasses(String nodeId) {
        AtomicInteger c = consecutivePasses.get(nodeId);
        return c != null ? c.get() : 0;
    }

    public int getConsecutiveFailures(String nodeId) {
        AtomicInteger c = consecutiveFailures.get(nodeId);
        return c != null ? c.get() : 0;
    }
}
