package com.loadbalancer.resilience;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps each backend node id to its circuit-breaker state.
 *
 * <p><b>Step 2 stub:</b> defaults to CLOSED (not open) so {@link com.loadbalancer.routing.SelectionEngine}
 * can be built and fully tested before the rolling-window error-rate logic is introduced.
 * Step 4 replaces this with a real per-node {@link CircuitBreakerState} tracker without
 * changing the {@code SelectionEngine} contract.</p>
 */
@Component
public class CircuitBreakerRegistry {

    private final ConcurrentHashMap<String, Boolean> openStates = new ConcurrentHashMap<>();

    /**
     * Returns {@code true} if the circuit for {@code nodeId} is OPEN and the
     * node must be excluded from backend selection.
     */
    public boolean isOpen(String nodeId) {
        return openStates.getOrDefault(nodeId, false);
    }

    /**
     * Helper for testing or administrative circuit state control.
     */
    public void setOpen(String nodeId, boolean open) {
        if (open) {
            openStates.put(nodeId, true);
        } else {
            openStates.remove(nodeId);
        }
    }
}
