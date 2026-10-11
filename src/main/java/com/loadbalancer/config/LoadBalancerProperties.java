package com.loadbalancer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Strongly-typed, externalisable configuration for the load balancer gateway.
 *
 * <p>All values are backed by {@code application.yml} under the {@code lb:} prefix.
 * Spring injects them at startup; no magic numbers live in business-logic code.</p>
 */
@Component
@ConfigurationProperties(prefix = "lb")
public class LoadBalancerProperties {

    // ── Backend registration ──────────────────────────────────────────────

    /**
     * K constant: maxCapacity = weight × K.
     * A node with weight=5 gets maxCapacity=500 at the default K=100.
     */
    private int capacityPerWeightUnit = 100;

    /**
     * Maximum allowed fractional deviation between a caller-supplied
     * maxCapacity and the ideal weight × K value, before registration is rejected.
     * 0.10 = 10 % tolerance.
     */
    private double maxCapacityDeviationFraction = 0.10;

    // ── Rate limiter ──────────────────────────────────────────────────────

    /** Baseline tokens-per-second refill rate when all backends are fully healthy. */
    private int rateLimitBaselineRps = 1000;

    // ── Proxy timeouts ────────────────────────────────────────────────────

    /** Max time (ms) to establish a TCP connection to a backend. */
    private long connectTimeoutMs = 3_000;

    /** Max time (ms) to wait for the backend to send the first response byte. */
    private long responseTimeoutMs = 30_000;

    // ── Circuit breaker ───────────────────────────────────────────────────

    /** Rolling window size (number of requests) for error-rate calculation. */
    private int circuitBreakerWindowSize = 20;

    /** Error rate threshold (0–1) above which the circuit trips OPEN. */
    private double circuitBreakerErrorThreshold = 0.50;

    /** Time (ms) the circuit stays OPEN before moving to HALF-OPEN. */
    private long circuitBreakerOpenDurationMs = 30_000;

    // ── Cache ─────────────────────────────────────────────────────────────

    /** Maximum number of entries in the Caffeine response cache. */
    private long cacheMaxSize = 10_000;

    /** TTL (seconds) for cached responses. */
    private long cacheTtlSeconds = 60;

    // ── Retry ─────────────────────────────────────────────────────────────

    /** Maximum number of retry attempts for idempotent requests. */
    private int maxRetries = 2;

    // ── Layer 4 TCP Tunnel ────────────────────────────────────────────────

    /** Dedicated raw TCP listener port for L4 TLS/TCP tunneling (default: 8443). */
    private int tunnelPort = 8443;

    /** Whether the L4 TCP Tunnel proxy is active. */
    private boolean tunnelEnabled = true;

    // ── Getters & Setters (required by @ConfigurationProperties) ─────────

    public int getCapacityPerWeightUnit()                { return capacityPerWeightUnit; }
    public void setCapacityPerWeightUnit(int v)          { this.capacityPerWeightUnit = v; }

    public double getMaxCapacityDeviationFraction()              { return maxCapacityDeviationFraction; }
    public void setMaxCapacityDeviationFraction(double v)        { this.maxCapacityDeviationFraction = v; }

    public int getRateLimitBaselineRps()                 { return rateLimitBaselineRps; }
    public void setRateLimitBaselineRps(int v)           { this.rateLimitBaselineRps = v; }

    public long getConnectTimeoutMs()                    { return connectTimeoutMs; }
    public void setConnectTimeoutMs(long v)              { this.connectTimeoutMs = v; }

    public long getResponseTimeoutMs()                   { return responseTimeoutMs; }
    public void setResponseTimeoutMs(long v)             { this.responseTimeoutMs = v; }

    public int getCircuitBreakerWindowSize()             { return circuitBreakerWindowSize; }
    public void setCircuitBreakerWindowSize(int v)       { this.circuitBreakerWindowSize = v; }

    public double getCircuitBreakerErrorThreshold()      { return circuitBreakerErrorThreshold; }
    public void setCircuitBreakerErrorThreshold(double v){ this.circuitBreakerErrorThreshold = v; }

    public long getCircuitBreakerOpenDurationMs()        { return circuitBreakerOpenDurationMs; }
    public void setCircuitBreakerOpenDurationMs(long v)  { this.circuitBreakerOpenDurationMs = v; }

    public long getCacheMaxSize()                        { return cacheMaxSize; }
    public void setCacheMaxSize(long v)                  { this.cacheMaxSize = v; }

    public long getCacheTtlSeconds()                     { return cacheTtlSeconds; }
    public void setCacheTtlSeconds(long v)               { this.cacheTtlSeconds = v; }

    public int getMaxRetries()                           { return maxRetries; }
    public void setMaxRetries(int v)                     { this.maxRetries = v; }

    public int getTunnelPort()                           { return tunnelPort; }
    public void setTunnelPort(int v)                     { this.tunnelPort = v; }

    public boolean isTunnelEnabled()                     { return tunnelEnabled; }
    public void setTunnelEnabled(boolean v)              { this.tunnelEnabled = v; }
}
