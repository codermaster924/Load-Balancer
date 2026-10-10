package com.loadbalancer.routing;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.resilience.CircuitBreakerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for {@link SelectionEngine}:
 * <ul>
 *   <li>Eligibility filtering (unhealthy nodes, tripped circuit breakers)</li>
 *   <li>WLC score evaluation and tie-breaking by highest weight</li>
 *   <li>Max capacity constraints</li>
 *   <li>Smooth WRR overflow proportionality</li>
 *   <li>Concurrent slot reservations under thread contention</li>
 * </ul>
 */
class SelectionEngineTest {

    private LoadBalancerProperties props;
    private BackendRegistry backendRegistry;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private SelectionEngine engine;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCapacityPerWeightUnit(10); // K=10: weight 1 -> cap 10, weight 5 -> cap 50
        props.setMaxCapacityDeviationFraction(0.10);

        backendRegistry = new BackendRegistry(props);
        circuitBreakerRegistry = new CircuitBreakerRegistry();
        engine = new SelectionEngine(backendRegistry, circuitBreakerRegistry);
    }

    @Test
    @DisplayName("returns empty optional and throws 503 when no backends are registered")
    void emptyRegistryReturnsEmpty() {
        Optional<ServerNode> selected = engine.selectNode();
        assertTrue(selected.isEmpty());

        assertThrows(NoAvailableBackendException.class, () -> engine.selectNodeOrThrow());
    }

    @Test
    @DisplayName("filters out unhealthy nodes")
    void filtersUnhealthyNodes() {
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:8081", 5, 0);
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:8082", 2, 0);

        n1.setHealthy(false);

        // n1 is unhealthy -> only n2 should be selected
        Optional<ServerNode> selected = engine.selectNode();
        assertTrue(selected.isPresent());
        assertEquals("n2", selected.get().getId());
    }

    @Test
    @DisplayName("filters out nodes with open circuit breakers")
    void filtersOpenCircuitBreakers() {
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:8081", 5, 0);
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:8082", 2, 0);

        circuitBreakerRegistry.setOpen("n1", true);

        // n1 circuit is open -> only n2 is eligible
        Optional<ServerNode> selected = engine.selectNode();
        assertTrue(selected.isPresent());
        assertEquals("n2", selected.get().getId());
    }

    @Test
    @DisplayName("returns empty when all nodes are either unhealthy or circuit-tripped")
    void allFilteredOutReturnsEmpty() {
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:8081", 5, 0);
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:8082", 2, 0);

        n1.setHealthy(false);
        circuitBreakerRegistry.setOpen("n2", true);

        assertTrue(engine.selectNode().isEmpty());
        assertThrows(NoAvailableBackendException.class, () -> engine.selectNodeOrThrow());
    }

    @Test
    @DisplayName("WLC breaks initial ties (0 connections) by picking node with higher weight")
    void wlcTieBreakByHighestWeight() {
        // n1: weight 5, active 0 -> score 0.0
        // n2: weight 1, active 0 -> score 0.0
        backendRegistry.register("n1", "http://localhost:8081", 5, 0);
        backendRegistry.register("n2", "http://localhost:8082", 1, 0);

        ServerNode selected = engine.selectNodeOrThrow();
        assertEquals("n1", selected.getId());
        assertEquals(1, selected.getActiveConnections());
    }

    @Test
    @DisplayName("WLC routes proportional to weight according to activeConnections / weight")
    void wlcProportionalDistribution() {
        // n1 (weight 5), n2 (weight 1)
        // K=10, so maxCapacity is 50 and 10
        backendRegistry.register("n1", "http://localhost:8081", 5, 50);
        backendRegistry.register("n2", "http://localhost:8082", 1, 10);

        // Request 1: both score 0.0 -> tie-break by weight gives n1. (n1 active: 1, score: 0.2; n2 active: 0, score: 0.0)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 2: n2 score is 0.0 vs n1 score 0.2 -> n2 wins. (n1 active: 1, score: 0.2; n2 active: 1, score: 1.0)
        assertEquals("n2", engine.selectNodeOrThrow().getId());

        // Request 3: n1 score is 0.2 vs n2 score 1.0 -> n1 wins. (n1 active: 2, score: 0.4)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 4: n1 score is 0.4 vs n2 score 1.0 -> n1 wins. (n1 active: 3, score: 0.6)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 5: n1 score is 0.6 vs n2 score 1.0 -> n1 wins. (n1 active: 4, score: 0.8)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 6: n1 score is 0.8 vs n2 score 1.0 -> n1 wins. (n1 active: 5, score: 1.0)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 7: both score 1.0 -> tie-break by weight gives n1! (n1 active: 6, score: 1.2)
        assertEquals("n1", engine.selectNodeOrThrow().getId());

        // Request 8: n2 score is 1.0 vs n1 score 1.2 -> n2 wins!
        assertEquals("n2", engine.selectNodeOrThrow().getId());
    }

    @Test
    @DisplayName("WLC respects maxCapacity and bypasses saturated node")
    void wlcRespectsMaxCapacity() {
        // Set K=1 so weight 5 -> cap 5, weight 1 -> cap 1
        props.setCapacityPerWeightUnit(1);
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:8081", 5, 0); // cap = 5
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:8082", 1, 0); // cap = 1

        // Saturate n1 (activeConnections = 5 == maxCapacity)
        for (int i = 0; i < 5; i++) {
            n1.incrementConnections();
        }
        assertEquals(5, n1.getActiveConnections());

        // Even though n1 has higher weight, its capacity is exhausted; n2 must be chosen
        ServerNode selected = engine.selectNodeOrThrow();
        assertEquals("n2", selected.getId());
        assertEquals(1, selected.getActiveConnections());
    }

    @Test
    @DisplayName("Smooth WRR overflow triggers when ALL eligible nodes are at or over maxCapacity")
    void smoothWrrOverflowWhenAllSaturated() {
        // K=1: weight 5 -> cap 5, weight 1 -> cap 1
        props.setCapacityPerWeightUnit(1);
        ServerNode n1 = backendRegistry.register("n1", "http://localhost:8081", 5, 0);
        ServerNode n2 = backendRegistry.register("n2", "http://localhost:8082", 1, 0);

        // Saturate both nodes to their maxCapacity
        for (int i = 0; i < 5; i++) n1.incrementConnections();
        n2.incrementConnections();

        assertEquals(5, n1.getActiveConnections());
        assertEquals(1, n2.getActiveConnections());

        // Overflow kicks in. Over 6 requests with weights (5, 1),
        // smooth WRR should pick n1 5 times and n2 1 time!
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            ServerNode chosen = engine.selectNodeOrThrow();
            counts.merge(chosen.getId(), 1, Integer::sum);
        }

        assertEquals(5, counts.get("n1"));
        assertEquals(1, counts.get("n2"));

        // Both nodes had their connections incremented past maxCapacity
        assertEquals(10, n1.getActiveConnections()); // 5 + 5
        assertEquals(2, n2.getActiveConnections());  // 1 + 1
    }

    @Test
    @DisplayName("Smooth WRR interleaves selections smoothly across cycles")
    void smoothWrrInterleaving() {
        props.setCapacityPerWeightUnit(1);
        ServerNode a = backendRegistry.register("A", "http://localhost:8081", 4, 0); // cap 4
        ServerNode b = backendRegistry.register("B", "http://localhost:8082", 2, 0); // cap 2
        ServerNode c = backendRegistry.register("C", "http://localhost:8083", 1, 0); // cap 1

        // Saturate all nodes
        for (int i = 0; i < 4; i++) a.incrementConnections();
        for (int i = 0; i < 2; i++) b.incrementConnections();
        c.incrementConnections();

        // 7 overflow selections with weights 4, 2, 1 (total = 7)
        List<String> order = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            order.add(engine.selectNodeOrThrow().getId());
        }

        // Total distribution must be A: 4, B: 2, C: 1
        long countA = order.stream().filter("A"::equals).count();
        long countB = order.stream().filter("B"::equals).count();
        long countC = order.stream().filter("C"::equals).count();

        assertEquals(4, countA);
        assertEquals(2, countB);
        assertEquals(1, countC);
    }

    @Test
    @DisplayName("Concurrent selections maintain strict CAS accounting without lost updates")
    void concurrentReservations() throws Exception {
        int threads = 8;
        int requestsPerThread = 50;
        int totalRequests = threads * requestsPerThread;

        props.setCapacityPerWeightUnit(500); // cap: 1500, 1000, 500
        backendRegistry.register("n1", "http://localhost:8081", 3, 0);
        backendRegistry.register("n2", "http://localhost:8082", 2, 0);
        backendRegistry.register("n3", "http://localhost:8083", 1, 0);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.await();
                    for (int i = 0; i < requestsPerThread; i++) {
                        ServerNode node = engine.selectNodeOrThrow();
                        assertNotNull(node);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        latch.countDown(); // start all threads simultaneously

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        int sumActive = backendRegistry.allNodes().stream()
                .mapToInt(ServerNode::getActiveConnections)
                .sum();

        assertEquals(totalRequests, sumActive, "Total active connections must exactly match total selections made");
    }
}
