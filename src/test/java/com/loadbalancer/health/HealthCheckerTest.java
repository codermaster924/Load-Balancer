package com.loadbalancer.health;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.routing.BackendRegistry;
import com.loadbalancer.routing.ServerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit and slice tests for {@link HealthChecker}:
 * <ul>
 *   <li>HTTP 200 health check pass</li>
 *   <li>Flapping protection: 2 consecutive failures mark unhealthy</li>
 *   <li>Flapping protection: 2 consecutive passes restore to healthy</li>
 *   <li>EWMA health score calculation</li>
 *   <li>Fleet-wide aggregate health score</li>
 * </ul>
 */
class HealthCheckerTest {

    private LoadBalancerProperties props;
    private BackendRegistry backendRegistry;
    private HealthChecker healthChecker;

    private DisposableServer mockBackend;
    private final AtomicInteger responseStatusCode = new AtomicInteger(200);

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCapacityPerWeightUnit(100);
        backendRegistry = new BackendRegistry(props);

        WebClient webClient = WebClient.builder().build();
        healthChecker = new HealthChecker(backendRegistry, webClient);

        responseStatusCode.set(200);
        mockBackend = HttpServer.create()
                .port(0)
                .route(routes -> routes.get("/health", (req, res) ->
                        res.status(responseStatusCode.get()).sendString(Mono.just("OK"))))
                .bindNow();
    }

    @AfterEach
    void tearDown() {
        if (mockBackend != null) {
            mockBackend.disposeNow();
        }
    }

    @Test
    @DisplayName("node remains healthy when /health returns 200 OK")
    void healthyNodePassesCheck() {
        ServerNode node = backendRegistry.register("b1", "http://localhost:" + mockBackend.port(), 1, 0);
        assertTrue(node.isHealthy());

        healthChecker.checkNode(node).block();

        assertTrue(node.isHealthy());
        assertEquals(1, healthChecker.getConsecutivePasses("b1"));
        assertEquals(0, healthChecker.getConsecutiveFailures("b1"));
        assertEquals(100.0, node.getHealthScore());
    }

    @Test
    @DisplayName("flapping protection: 1 failure keeps node healthy, 2nd consecutive failure marks it unhealthy")
    void flappingProtectionTwoFailuresMarkUnhealthy() {
        ServerNode node = backendRegistry.register("b1", "http://localhost:" + mockBackend.port(), 1, 0);
        assertTrue(node.isHealthy());

        // Make backend fail
        responseStatusCode.set(500);

        // Check 1: 1 failure
        healthChecker.checkNode(node).block();
        assertTrue(node.isHealthy(), "Single failure must NOT mark node unhealthy (flapping protection)");
        assertEquals(1, healthChecker.getConsecutiveFailures("b1"));
        assertTrue(node.getHealthScore() < 100.0, "Health score must drop after failure");

        // Check 2: 2nd consecutive failure
        healthChecker.checkNode(node).block();
        assertFalse(node.isHealthy(), "2 consecutive failures must mark node unhealthy");
        assertEquals(2, healthChecker.getConsecutiveFailures("b1"));
    }

    @Test
    @DisplayName("flapping protection: 2 consecutive passes required to restore unhealthy node")
    void flappingProtectionTwoPassesRestoreHealth() {
        ServerNode node = backendRegistry.register("b1", "http://localhost:" + mockBackend.port(), 1, 0);

        // Fail 2 times to become unhealthy
        responseStatusCode.set(503);
        healthChecker.checkNode(node).block();
        healthChecker.checkNode(node).block();
        assertFalse(node.isHealthy());

        // Backend recovers
        responseStatusCode.set(200);

        // Check 1: 1st pass -> remains unhealthy
        healthChecker.checkNode(node).block();
        assertFalse(node.isHealthy(), "Single pass must NOT immediately restore healthy (flapping protection)");
        assertEquals(1, healthChecker.getConsecutivePasses("b1"));

        // Check 2: 2nd consecutive pass -> restored to healthy!
        healthChecker.checkNode(node).block();
        assertTrue(node.isHealthy(), "2 consecutive passes must restore node to healthy");
        assertEquals(2, healthChecker.getConsecutivePasses("b1"));
    }

    @Test
    @DisplayName("calculates aggregate fleet health score across multiple nodes")
    void aggregateHealthScoreCalculation() {
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:" + mockBackend.port(), 1, 0);
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:" + mockBackend.port(), 1, 0);

        n1.setHealthScore(90.0);
        n2.setHealthScore(70.0);

        // Mean of 90 and 70 is 80.0
        assertEquals(80.0, healthChecker.getAggregateHealthScore(), 0.01);
    }
}
