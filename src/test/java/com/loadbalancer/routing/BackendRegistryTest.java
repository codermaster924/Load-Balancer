package com.loadbalancer.routing;

import com.loadbalancer.config.LoadBalancerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link BackendRegistry} registration logic.
 *
 * <p>No Spring context is loaded — the registry is constructed directly with a
 * {@link LoadBalancerProperties} instance to keep tests fast and focused.</p>
 */
class BackendRegistryTest {

    private LoadBalancerProperties props;
    private BackendRegistry registry;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        // K = 100, tolerance = 10 %
        props.setCapacityPerWeightUnit(100);
        props.setMaxCapacityDeviationFraction(0.10);
        registry = new BackendRegistry(props);
    }

    @Test
    @DisplayName("registers a node and derives maxCapacity from weight × K")
    void registersNodeAndDerivesCapacity() {
        ServerNode node = registry.register("n1", "http://localhost:8081", 5, 0);

        assertEquals("n1",                    node.getId());
        assertEquals("http://localhost:8081", node.getUrl());
        assertEquals(5,                        node.getWeight());
        assertEquals(500,                      node.getMaxCapacity()); // 5 × 100
        assertTrue(node.isHealthy());
        assertEquals(0, node.getActiveConnections());
    }

    @Test
    @DisplayName("accepts caller-supplied maxCapacity within 10 % of weight × K")
    void acceptsCapacityWithinTolerance() {
        // 5 × 100 = 500; 490 is within 10 %
        assertDoesNotThrow(() -> registry.register("n2", "http://localhost:8082", 5, 490));
    }

    @Test
    @DisplayName("rejects maxCapacity that deviates more than 10 % from weight × K")
    void rejectsCapacityOutsideTolerance() {
        // 5 × 100 = 500; 400 deviates 20 % — must be rejected
        assertThrows(IllegalArgumentException.class,
                () -> registry.register("n3", "http://localhost:8083", 5, 400));
    }

    @Test
    @DisplayName("rejects weight ≤ 0")
    void rejectsNonPositiveWeight() {
        assertThrows(IllegalArgumentException.class,
                () -> registry.register("n4", "http://localhost:8084", 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> registry.register("n5", "http://localhost:8085", -1, 0));
    }

    @Test
    @DisplayName("replacing an existing node id updates the registry atomically")
    void replacesExistingNode() {
        registry.register("n1", "http://localhost:8081", 5, 0);
        registry.register("n1", "http://localhost:9081", 3, 0); // same id, new url

        assertEquals(1, registry.size());
        assertEquals("http://localhost:9081", registry.findById("n1").orElseThrow().getUrl());
    }

    @Test
    @DisplayName("healthyCount reflects isHealthy flags")
    void healthyCountReflectsFlags() {
        ServerNode n1 = registry.register("n1", "http://localhost:8081", 1, 0);
        ServerNode n2 = registry.register("n2", "http://localhost:8082", 1, 0);

        assertEquals(2, registry.healthyCount());

        n1.setHealthy(false);
        assertEquals(1, registry.healthyCount());

        n2.setHealthy(false);
        assertEquals(0, registry.healthyCount());
    }

    @Test
    @DisplayName("allNodes returns an unmodifiable view")
    void allNodesIsUnmodifiable() {
        registry.register("n1", "http://localhost:8081", 1, 0);
        assertThrows(UnsupportedOperationException.class,
                () -> registry.allNodes().clear());
    }

    // ── ServerNode atomic helpers (tested here because they need no Spring) ──

    @Test
    @DisplayName("tryReserveSlot succeeds while under capacity and fails at capacity")
    void tryReserveSlotCasSemantics() {
        ServerNode node = new ServerNode("x", "http://x", 1, 3); // maxCapacity = 3

        assertTrue(node.tryReserveSlot());  // active = 1
        assertTrue(node.tryReserveSlot());  // active = 2
        assertTrue(node.tryReserveSlot());  // active = 3
        assertFalse(node.tryReserveSlot()); // at capacity — must fail
        assertEquals(3, node.getActiveConnections());
    }

    @Test
    @DisplayName("decrementConnections never goes below zero")
    void decrementDoesNotGoNegative() {
        ServerNode node = new ServerNode("x", "http://x", 1, 3);
        node.decrementConnections(); // already 0 — defensive guard must absorb this
        assertEquals(0, node.getActiveConnections());
    }

    @Test
    @DisplayName("wlcScore = activeConnections / weight")
    void wlcScoreFormula() {
        ServerNode node = new ServerNode("x", "http://x", 4, 400);
        node.tryReserveSlot(); // active = 1
        node.tryReserveSlot(); // active = 2
        // score = 2 / 4 = 0.5
        assertEquals(0.5, node.wlcScore(), 1e-9);
    }
}
