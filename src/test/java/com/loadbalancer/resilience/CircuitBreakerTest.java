package com.loadbalancer.resilience;

import com.loadbalancer.config.LoadBalancerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CircuitBreaker}:
 * <ul>
 *   <li>Window size and minimum sample evaluation (min 10 samples required)</li>
 *   <li>Error rate calculation (>= 50% trips OPEN)</li>
 *   <li>State transition from OPEN to HALF_OPEN after duration expires</li>
 *   <li>Probe permit handling in HALF_OPEN (exclusive permit)</li>
 *   <li>Probe recovery: success closes circuit, failure re-opens circuit</li>
 * </ul>
 */
class CircuitBreakerTest {

    private LoadBalancerProperties props;
    private CircuitBreaker cb;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCircuitBreakerWindowSize(20);
        props.setCircuitBreakerErrorThreshold(0.50);
        props.setCircuitBreakerOpenDurationMs(1000); // 1 second for testing

        cb = new CircuitBreaker("node-1", props);
    }

    @Test
    @DisplayName("starts in CLOSED state and accepts traffic")
    void startsClosed() {
        assertEquals(CircuitBreakerState.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
        assertTrue(cb.canAcceptTraffic());
    }

    @Test
    @DisplayName("does not trip OPEN before minimum 10 samples even if all fail")
    void requiresMinimumSamplesBeforeEvaluation() {
        // Record 9 errors (< 10 minimum samples)
        for (int i = 0; i < 9; i++) {
            cb.recordError();
        }

        // Even with 100% errors, it must NOT trip yet
        assertEquals(CircuitBreakerState.CLOSED, cb.getState());
        assertFalse(cb.isOpen());

        // 10th error triggers evaluation -> 10/10 = 100% >= 50% -> trips OPEN
        cb.recordError();
        assertEquals(CircuitBreakerState.OPEN, cb.getState());
        assertTrue(cb.isOpen());
        assertFalse(cb.canAcceptTraffic());
    }

    @Test
    @DisplayName("stays CLOSED if error rate is below threshold after 10 samples")
    void staysClosedWhenUnderThreshold() {
        // 4 errors, 6 successes = 40% error rate (< 50% threshold)
        for (int i = 0; i < 4; i++) cb.recordError();
        for (int i = 0; i < 6; i++) cb.recordSuccess();

        assertEquals(CircuitBreakerState.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
    }

    @Test
    @DisplayName("trips OPEN when error rate reaches or exceeds 50% after 10 samples")
    void tripsWhenThresholdExceeded() {
        // 5 errors, 5 successes = 50% error rate (>= 50% threshold)
        for (int i = 0; i < 5; i++) cb.recordError();
        for (int i = 0; i < 5; i++) cb.recordSuccess();

        assertEquals(CircuitBreakerState.OPEN, cb.getState());
        assertTrue(cb.isOpen());
        assertFalse(cb.canAcceptTraffic());
    }

    @Test
    @DisplayName("transitions from OPEN to HALF_OPEN after open duration expires")
    void transitionsToHalfOpenAfterTimeout() {
        for (int i = 0; i < 10; i++) cb.recordError();
        assertEquals(CircuitBreakerState.OPEN, cb.getState());

        // Fast-forward open timestamp to simulate elapsed duration
        cb.setOpenTimestampMs(System.currentTimeMillis() - 2000);

        // Accessing state or isOpen automatically transitions to HALF_OPEN
        assertEquals(CircuitBreakerState.HALF_OPEN, cb.getState());
        assertFalse(cb.isOpen());
        assertTrue(cb.canAcceptTraffic());
    }

    @Test
    @DisplayName("HALF_OPEN admits exactly one probe request via tryAcquireProbe")
    void halfOpenAdmitsSingleProbe() {
        for (int i = 0; i < 10; i++) cb.recordError();
        cb.setOpenTimestampMs(System.currentTimeMillis() - 2000);
        assertEquals(CircuitBreakerState.HALF_OPEN, cb.getState());

        // First probe acquires permit
        assertTrue(cb.tryAcquireProbe());

        // Subsequent probe while first is in flight is rejected
        assertFalse(cb.tryAcquireProbe());
        assertFalse(cb.canAcceptTraffic());

        // Releasing permit allows another probe
        cb.releaseProbePermit();
        assertTrue(cb.tryAcquireProbe());
    }

    @Test
    @DisplayName("successful probe in HALF_OPEN closes the circuit breaker")
    void successfulProbeClosesCircuit() {
        for (int i = 0; i < 10; i++) cb.recordError();
        cb.setOpenTimestampMs(System.currentTimeMillis() - 2000);
        assertEquals(CircuitBreakerState.HALF_OPEN, cb.getState());

        assertTrue(cb.tryAcquireProbe());
        cb.recordSuccess();

        assertEquals(CircuitBreakerState.CLOSED, cb.getState());
        assertFalse(cb.isOpen());
        assertTrue(cb.canAcceptTraffic());
    }

    @Test
    @DisplayName("failed probe in HALF_OPEN re-trips the circuit breaker back to OPEN")
    void failedProbeReopensCircuit() {
        for (int i = 0; i < 10; i++) cb.recordError();
        cb.setOpenTimestampMs(System.currentTimeMillis() - 2000);
        assertEquals(CircuitBreakerState.HALF_OPEN, cb.getState());

        assertTrue(cb.tryAcquireProbe());
        cb.recordError();

        assertEquals(CircuitBreakerState.OPEN, cb.getState());
        assertTrue(cb.isOpen());
        assertFalse(cb.canAcceptTraffic());
    }
}
