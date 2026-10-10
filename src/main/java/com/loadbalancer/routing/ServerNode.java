package com.loadbalancer.routing;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Immutable identity + mutable runtime state for a single backend server.
 *
 * <p>All mutable fields use lock-free atomics so they can be read and updated
 * safely from concurrent Netty event-loop threads without synchronisation blocks.</p>
 *
 * <p>Design note — equity over equality: {@code maxCapacity} MUST be set proportional
 * to {@code weight} (maxCapacity = weight × K). This invariant is enforced at
 * registration time by the backend registry.</p>
 */
public class ServerNode {

    // ── Immutable identity ────────────────────────────────────────────────

    /** Stable node identifier, e.g. "node-1". */
    private final String id;

    /** Base URL of the backend, e.g. "http://localhost:8081". */
    private final String url;

    /**
     * Relative computational power. Higher weight → higher share of traffic
     * and a higher capacity ceiling.
     */
    private final int weight;

    /**
     * Hard cap on simultaneous in-flight requests (= weight × K).
     * WLC uses this as an eligibility filter; WRR overflow may exceed it
     * temporarily while still accounting every connection.
     */
    private final int maxCapacity;

    // ── Mutable runtime state (lock-free atomics) ─────────────────────────

    /**
     * Count of currently in-flight requests routed to this node.
     * Incremented atomically before proxy dispatch; decremented in doFinally
     * on every exit path (success, error, timeout, client cancel).
     */
    private final AtomicInteger activeConnections = new AtomicInteger(0);

    /**
     * Whether the periodic health checker last found this node reachable.
     * The circuit breaker provides faster passive detection between 60 s checks.
     */
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    /**
     * Rolling health score in [0, 100]. Computed by the health checker and
     * fed into the adaptive rate limiter's refill calculation.
     */
    private volatile double healthScore = 100.0;

    /**
     * Per-node weight used by the Smooth WRR overflow algorithm.
     * Starts at 0; updated inside the single-writer WRR lock in the selection engine.
     * Declared volatile so the selection-engine reader always sees the latest value
     * without needing to hold the lock.
     */
    private volatile int currentWeight = 0;

    // ── Construction ──────────────────────────────────────────────────────

    /**
     * @param id          unique node identifier
     * @param url         base URL of the backend (no trailing slash)
     * @param weight      relative computational power (must be > 0)
     * @param maxCapacity hard cap on in-flight requests (must be > 0)
     */
    public ServerNode(String id, String url, int weight, int maxCapacity) {
        if (weight <= 0)       throw new IllegalArgumentException("weight must be > 0");
        if (maxCapacity <= 0)  throw new IllegalArgumentException("maxCapacity must be > 0");
        this.id          = id;
        this.url         = url;
        this.weight      = weight;
        this.maxCapacity = maxCapacity;
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    public String  getId()               { return id; }
    public String  getUrl()              { return url; }
    public int     getWeight()           { return weight; }
    public int     getMaxCapacity()      { return maxCapacity; }
    public int     getActiveConnections(){ return activeConnections.get(); }
    public boolean isHealthy()           { return healthy.get(); }
    public double  getHealthScore()      { return healthScore; }
    public int     getCurrentWeight()    { return currentWeight; }

    // ── Mutators ──────────────────────────────────────────────────────────

    public void setHealthy(boolean healthy)         { this.healthy.set(healthy); }
    public void setHealthScore(double healthScore)  { this.healthScore = healthScore; }
    public void setCurrentWeight(int currentWeight) { this.currentWeight = currentWeight; }

    /**
     * Atomically increments active connections unconditionally.
     * Used by the WRR overflow path where connections may exceed maxCapacity.
     */
    public void incrementConnections() {
        activeConnections.incrementAndGet();
    }

    /**
     * Decrements active connections. Called from doFinally on every exit path.
     * Guards against going below zero in case of a double-decrement bug.
     */
    public void decrementConnections() {
        // CAS loop guards against spurious negative values from races or bugs
        int current;
        do {
            current = activeConnections.get();
            if (current <= 0) return; // already at floor — defensive guard
        } while (!activeConnections.compareAndSet(current, current - 1));
    }

    /**
     * Attempts a capacity-checked atomic increment (the WLC reservation step).
     *
     * <p>The check {@code activeConnections < maxCapacity} and the increment are
     * fused into a single CAS so no other thread can slip in between them.</p>
     *
     * @return {@code true} if the slot was reserved; {@code false} if the node
     *         was already at or over capacity (caller must re-evaluate or overflow)
     */
    public boolean tryReserveSlot() {
        int current;
        do {
            current = activeConnections.get();
            if (current >= maxCapacity) return false; // over capacity — let caller decide
        } while (!activeConnections.compareAndSet(current, current + 1));
        return true;
    }

    /**
     * WLC score: lower is better. Ties are broken externally by highest weight.
     */
    public double wlcScore() {
        return (double) activeConnections.get() / weight;
    }

    @Override
    public String toString() {
        return String.format("ServerNode{id='%s', url='%s', weight=%d, maxCapacity=%d, " +
                             "active=%d, healthy=%b, score=%.3f}",
                id, url, weight, maxCapacity, activeConnections.get(), healthy.get(), wlcScore());
    }
}
