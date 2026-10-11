package com.loadbalancer.resilience;

import com.loadbalancer.config.LoadBalancerProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry mapping each backend {@code nodeId} to its {@link CircuitBreaker}.
 *
 * <p>All subsystems (selection engine, proxy handler, and metrics endpoint) interact
 * with circuit breaker states through this component.</p>
 */
@Component
public class CircuitBreakerRegistry {

    private final LoadBalancerProperties props;
    private final ConcurrentHashMap<String, CircuitBreaker> breakers = new ConcurrentHashMap<>();

    public CircuitBreakerRegistry(LoadBalancerProperties props) {
        this.props = props;
    }

    /**
     * Default constructor for test harnesses where default properties suffice.
     */
    public CircuitBreakerRegistry() {
        this(new LoadBalancerProperties());
    }

    /**
     * Returns the circuit breaker for {@code nodeId}, creating one if not present.
     */
    public CircuitBreaker getOrCreate(String nodeId) {
        return breakers.computeIfAbsent(nodeId, id -> new CircuitBreaker(id, props));
    }

    /**
     * Returns {@code true} if the circuit for {@code nodeId} is currently OPEN
     * and the node must be excluded from traffic routing.
     */
    public boolean isOpen(String nodeId) {
        CircuitBreaker cb = breakers.get(nodeId);
        return cb != null && cb.isOpen();
    }

    /**
     * Returns {@code true} if the circuit breaker can accept traffic.
     */
    public boolean canAcceptTraffic(String nodeId) {
        CircuitBreaker cb = breakers.get(nodeId);
        return cb == null || cb.canAcceptTraffic();
    }

    public boolean tryAcquireProbe(String nodeId) {
        return getOrCreate(nodeId).tryAcquireProbe();
    }

    public void releaseProbePermit(String nodeId) {
        CircuitBreaker cb = breakers.get(nodeId);
        if (cb != null) {
            cb.releaseProbePermit();
        }
    }

    /**
     * Records a successful upstream request for {@code nodeId}.
     */
    public void recordSuccess(String nodeId) {
        getOrCreate(nodeId).recordSuccess();
    }

    /**
     * Records a failed upstream request (connect error, timeout, or 5xx) for {@code nodeId}.
     */
    public void recordError(String nodeId) {
        getOrCreate(nodeId).recordError();
    }

    /**
     * Returns the current {@link CircuitBreakerState} for {@code nodeId}.
     */
    public CircuitBreakerState getState(String nodeId) {
        return getOrCreate(nodeId).getState();
    }

    /**
     * Directly overrides the circuit breaker state for a node (used in testing).
     */
    public void setOpen(String nodeId, boolean open) {
        CircuitBreaker cb = getOrCreate(nodeId);
        if (open) {
            cb.recordError();
            // Force open timestamp
            cb.setOpenTimestampMs(System.currentTimeMillis());
            // Set state directly by triggering threshold
            for (int i = 0; i < props.getCircuitBreakerWindowSize(); i++) {
                cb.recordError();
            }
        } else {
            cb.resetToClosed();
        }
    }
}
