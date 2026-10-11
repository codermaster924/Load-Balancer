package com.loadbalancer.resilience;

import com.loadbalancer.config.LoadBalancerProperties;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe, lock-free per-node Circuit Breaker.
 *
 * <p>Implements the requirements from specification Section 3.A:
 * <ul>
 *   <li>Count-based sliding window: last {@code lb.circuit-breaker-window-size} (20) request outcomes.</li>
 *   <li>Evaluation occurs only after at least 10 outcomes are recorded in the window.</li>
 *   <li>Errors: connect failures, timeouts, and 5xx responses. 4xx is not considered an error.</li>
 *   <li>When error rate &ge; threshold (50 %), transitions to {@link CircuitBreakerState#OPEN}.</li>
 *   <li>Stays OPEN for {@code lb.circuit-breaker-open-duration-ms} (30s), then transitions to {@link CircuitBreakerState#HALF_OPEN}.</li>
 *   <li>HALF_OPEN admits a single probe request via {@link #tryAcquireProbe()};
 *       probe success resets to CLOSED, probe failure re-opens the breaker.</li>
 * </ul>
 * </p>
 */
public class CircuitBreaker {

    /** Minimum number of recorded outcomes required before evaluating the error threshold. */
    private static final int MIN_EVALUATION_SAMPLES = 10;

    private final String nodeId;
    private final int windowSize;
    private final double errorThreshold;
    private final long openDurationMs;

    private final AtomicReference<CircuitBreakerState> state = new AtomicReference<>(CircuitBreakerState.CLOSED);
    private final AtomicLong openTimestampMs = new AtomicLong(0);

    /** Ring buffer of outcomes: 1 = error, 0 = success. */
    private final int[] outcomeBuffer;
    private final AtomicInteger sampleCount = new AtomicInteger(0);
    private final AtomicInteger ringIndex = new AtomicInteger(0);

    /** Atomic permit allowing at most one concurrent probe request in HALF_OPEN state. */
    private final AtomicBoolean probeInFlight = new AtomicBoolean(false);

    public CircuitBreaker(String nodeId, LoadBalancerProperties props) {
        this.nodeId = nodeId;
        this.windowSize = Math.max(1, props.getCircuitBreakerWindowSize());
        this.errorThreshold = props.getCircuitBreakerErrorThreshold();
        this.openDurationMs = props.getCircuitBreakerOpenDurationMs();
        this.outcomeBuffer = new int[windowSize];
    }

    /**
     * Checks if the circuit is open.
     * If the OPEN timeout has elapsed, automatically advances the state to {@link CircuitBreakerState#HALF_OPEN}.
     */
    public boolean isOpen() {
        CircuitBreakerState current = state.get();
        if (current == CircuitBreakerState.OPEN) {
            long now = System.currentTimeMillis();
            if (now - openTimestampMs.get() >= openDurationMs) {
                if (state.compareAndSet(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN)) {
                    probeInFlight.set(false);
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Returns true if this circuit can accept traffic (CLOSED, or HALF_OPEN with available probe).
     */
    public boolean canAcceptTraffic() {
        CircuitBreakerState current = getState();
        if (current == CircuitBreakerState.CLOSED) {
            return true;
        }
        if (current == CircuitBreakerState.HALF_OPEN) {
            return !probeInFlight.get();
        }
        return false;
    }

    /**
     * Releases probe permit if slot reservation failed.
     */
    public void releaseProbePermit() {
        if (getState() == CircuitBreakerState.HALF_OPEN) {
            probeInFlight.set(false);
        }
    }

    /**
     * In HALF_OPEN state, attempts to acquire an exclusive probe permit.
     *
     * @return {@code true} if caller is permitted to probe the backend; {@code false} if probe is already in-flight
     */
    public boolean tryAcquireProbe() {
        if (getState() == CircuitBreakerState.HALF_OPEN) {
            return probeInFlight.compareAndSet(false, true);
        }
        return false;
    }

    /**
     * Records a successful request outcome.
     * In HALF_OPEN, a success immediately recovers the circuit back to CLOSED.
     */
    public void recordSuccess() {
        CircuitBreakerState current = state.get();
        if (current == CircuitBreakerState.HALF_OPEN) {
            resetToClosed();
            return;
        }
        recordOutcome(0);
        evaluateErrorRate();
    }

    /**
     * Records an error outcome (connect failure, timeout, or 5xx).
     * In HALF_OPEN, a failure immediately trips the circuit back to OPEN.
     */
    public void recordError() {
        CircuitBreakerState current = state.get();
        if (current == CircuitBreakerState.HALF_OPEN) {
            tripToOpen();
            return;
        }

        recordOutcome(1);
        evaluateErrorRate();
    }

    private synchronized void recordOutcome(int outcome) {
        int idx = ringIndex.getAndIncrement();
        outcomeBuffer[Math.abs(idx % windowSize)] = outcome;
        if (sampleCount.get() < windowSize) {
            sampleCount.incrementAndGet();
        }
    }

    private synchronized void evaluateErrorRate() {
        int samples = sampleCount.get();
        if (samples < MIN_EVALUATION_SAMPLES) {
            return;
        }

        int errors = 0;
        int evaluated = Math.min(samples, windowSize);
        for (int i = 0; i < evaluated; i++) {
            errors += outcomeBuffer[i];
        }

        double rate = (double) errors / evaluated;
        if (rate >= errorThreshold) {
            tripToOpen();
        }
    }

    private void tripToOpen() {
        openTimestampMs.set(System.currentTimeMillis());
        state.set(CircuitBreakerState.OPEN);
        probeInFlight.set(false);
    }

    public synchronized void resetToClosed() {
        state.set(CircuitBreakerState.CLOSED);
        sampleCount.set(0);
        ringIndex.set(0);
        for (int i = 0; i < windowSize; i++) {
            outcomeBuffer[i] = 0;
        }
        probeInFlight.set(false);
    }

    public CircuitBreakerState getState() {
        // Trigger timeout advancement if open duration expired
        isOpen();
        return state.get();
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getOpenTimestampMs() {
        return openTimestampMs.get();
    }

    /** Helper for tests to simulate elapsed time. */
    public void setOpenTimestampMs(long timestamp) {
        this.openTimestampMs.set(timestamp);
    }
}
