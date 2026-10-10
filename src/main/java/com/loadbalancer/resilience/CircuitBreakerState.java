package com.loadbalancer.resilience;

/**
 * The three states of a per-node circuit breaker.
 *
 * <p>CLOSED is normal operation. OPEN short-circuits all requests to protect
 * a failing backend. HALF_OPEN allows a limited probe to test recovery.</p>
 */
public enum CircuitBreakerState {

    /** Normal — all requests flow through to the backend. */
    CLOSED,

    /** Tripped — requests are rejected immediately without contacting the backend. */
    OPEN,

    /** Recovery probe — a limited traffic slice is forwarded to test the backend. */
    HALF_OPEN
}
