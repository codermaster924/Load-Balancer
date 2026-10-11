package com.loadbalancer.resilience;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.health.HealthChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Adaptive, globally-scoped token bucket rate limiter.
 *
 * <p>Implements specification Section 3.C:
 * <ul>
 *   <li>Single global token bucket keyed to {@code lb.rate-limit-baseline-rps}.</li>
 *   <li>Effective refill rate = {@code baseline × (avgHealthScore / 100)},
 *       never below 10% of the baseline so the gateway never fully stalls.</li>
 *   <li>Bucket capacity (burst ceiling) = one second of baseline refill tokens.</li>
 *   <li>Rejected requests must receive {@code 429 Too Many Requests}
 *       with a {@code Retry-After: 1} header (enforced by the caller).</li>
 * </ul>
 * </p>
 *
 * <h3>Thread safety</h3>
 * <p>Token consumption uses a CAS loop on an {@link AtomicLong} so no lock is
 * held on the hot path. The refill is driven by a scheduled thread every 100 ms
 * (10 refills/sec) to keep token granularity fine without blocking request threads.</p>
 */
@Component
public class RateLimiter {

    private static final Logger log = LoggerFactory.getLogger(RateLimiter.class);

    /** Floor: effective refill is at least this fraction of baseline even if all nodes are sick. */
    private static final double MIN_HEALTH_FRACTION = 0.10;

    /** Refill interval in milliseconds. Tokens are added in small increments 10×/sec. */
    private static final long REFILL_INTERVAL_MS = 100;

    private final LoadBalancerProperties props;
    private final HealthChecker healthChecker;

    /**
     * Current token count.
     * Ceiling = {@code baseline} (one second of full-speed traffic).
     * Stored as a long to allow atomic CAS without floating-point races.
     */
    private final AtomicLong tokens;

    public RateLimiter(LoadBalancerProperties props, HealthChecker healthChecker) {
        this.props         = props;
        this.healthChecker = healthChecker;
        // Initialise the bucket full
        this.tokens = new AtomicLong(props.getRateLimitBaselineRps());
    }

    // ── Public API ────────────────────────────────────────────────────────

    /**
     * Attempts to consume one token.
     *
     * @return {@code true} if the request is allowed; {@code false} if the bucket
     *         is empty and the caller should return {@code 429}.
     */
    public boolean tryAcquire() {
        long current;
        do {
            current = tokens.get();
            if (current <= 0) {
                return false;          // bucket empty — reject
            }
        } while (!tokens.compareAndSet(current, current - 1));
        return true;
    }

    // ── Scheduled refill ──────────────────────────────────────────────────

    /**
     * Refills the bucket every {@value REFILL_INTERVAL_MS} ms.
     *
     * <p>Tokens added per tick = effectiveRps / (1000 / REFILL_INTERVAL_MS).
     * The effective RPS is scaled by fleet health so a degraded fleet
     * automatically admits less traffic, protecting remaining backends.</p>
     */
    @Scheduled(fixedRate = REFILL_INTERVAL_MS)
    public void refill() {
        int baseline       = props.getRateLimitBaselineRps();
        double healthRatio = Math.max(MIN_HEALTH_FRACTION,
                healthChecker.getAggregateHealthScore() / 100.0);
        double effectiveRps = baseline * healthRatio;

        // Tokens to add this tick (fractional — accumulate over ticks naturally)
        long tokensPerTick = Math.max(1L,
                Math.round(effectiveRps / (1000.0 / REFILL_INTERVAL_MS)));

        long capacity = baseline; // burst ceiling = 1 second of baseline
        tokens.updateAndGet(current -> Math.min(current + tokensPerTick, capacity));
    }

    // ── Observability ─────────────────────────────────────────────────────

    /** Current token level — useful for metrics and tests. */
    public long getTokenCount() {
        return tokens.get();
    }

    /**
     * Current effective RPS cap, factoring in fleet health.
     * Exposed for the {@code /lb/metrics} endpoint.
     */
    public double getEffectiveRps() {
        int baseline       = props.getRateLimitBaselineRps();
        double healthRatio = Math.max(MIN_HEALTH_FRACTION,
                healthChecker.getAggregateHealthScore() / 100.0);
        return baseline * healthRatio;
    }
}
