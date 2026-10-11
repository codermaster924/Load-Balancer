package com.loadbalancer.resilience;

import com.loadbalancer.config.LoadBalancerProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe global retry budget.
 *
 * <p>Enforces the rule from specification Section 3.D:
 * <i>"A global retry budget (default 10 % of recent request volume) caps total retries
 * so retries cannot amplify an outage."</i></p>
 *
 * <p>Uses lock-free fixed-point arithmetic to track tokens. Every incoming request
 * adds a fractional token (10 % = 0.1 tokens). Every retry attempts to acquire 1.0 token.
 * A baseline floor ensures low-volume periods still allow legitimate retry attempts.</p>
 */
@Component
public class RetryBudget {

    /** Scale factor for fixed-point arithmetic (1000 units = 1.0 token). */
    private static final long SCALE = 1_000L;

    /** Maximum token bucket burst capacity (equivalent to 50 retries). */
    private static final long MAX_BURST_TOKENS = 50 * SCALE;

    /** Minimum floor tokens to allow initial retries during startup or low traffic (10 retries). */
    private static final long INITIAL_TOKENS = 10 * SCALE;

    /** Stored tokens in fixed-point units. */
    private final AtomicLong tokens;

    /** Tokens added per request (default 10 % = 100 fixed-point units). */
    private final long tokensPerRequest;

    public RetryBudget(LoadBalancerProperties props) {
        // 10 % of incoming requests converted to retry allowance
        this.tokensPerRequest = (long) (0.10 * SCALE);
        this.tokens = new AtomicLong(INITIAL_TOKENS);
    }

    /**
     * Records an incoming request, increasing the retry allowance.
     */
    public void recordRequest() {
        tokens.updateAndGet(current -> Math.min(MAX_BURST_TOKENS, current + tokensPerRequest));
    }

    /**
     * Attempts to acquire 1.0 token to permit an idempotent retry.
     *
     * @return {@code true} if budget was available and consumed; {@code false} if budget is exhausted
     */
    public boolean tryAcquire() {
        long current;
        do {
            current = tokens.get();
            if (current < SCALE) {
                return false;
            }
        } while (!tokens.compareAndSet(current, current - SCALE));
        return true;
    }

    /** Returns the approximate current available retry tokens. */
    public double getAvailableTokens() {
        return (double) tokens.get() / SCALE;
    }
}
