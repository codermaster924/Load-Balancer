package com.loadbalancer.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.loadbalancer.resilience.CircuitBreakerRegistry;

/**
 * Custom routing engine implementing the hybrid load balancing algorithm:
 * <ul>
 *   <li><b>Eligibility Filter:</b> {@code isHealthy == true} and circuit breaker not {@code OPEN}.</li>
 *   <li><b>Weighted Least Connections (WLC):</b> Prioritises nodes under {@code maxCapacity}
 *       scored by {@code activeConnections / weight}. Ties broken by highest {@code weight}.
 *       Atomically reserves a capacity slot via CAS loop.</li>
 *   <li><b>Smooth Weighted Round Robin (WRR) Overflow:</b> Engaged when all eligible nodes are at
 *       or above {@code maxCapacity}. Distributes overflow traffic proportionally to weight
 *       using nginx's Smooth WRR algorithm.</li>
 * </ul>
 *
 * <p>Equity over equality: both the primary WLC path and the WRR overflow path distribute
 * requests strictly proportional to node weights.</p>
 */
@Component
public class SelectionEngine {

    private static final Logger log = LoggerFactory.getLogger(SelectionEngine.class);

    /** Minimum gap between "no eligible backend" warnings, so an outage cannot flood the log. */
    private static final long EMPTY_POOL_WARN_INTERVAL_MS = 10_000;

    private final BackendRegistry backendRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    /**
     * Lightweight lock guarding the Smooth WRR state mutations during overflow.
     * Per specification, this path is rare, so a single lock avoids concurrency anomalies
     * in currentWeight calculations.
     */
    private final ReentrantLock wrrLock = new ReentrantLock();

    /** Wall-clock time (ms) of the last "no eligible backend" warning. */
    private final AtomicLong lastEmptyPoolWarnMs = new AtomicLong(0);

    public SelectionEngine(BackendRegistry backendRegistry, CircuitBreakerRegistry circuitBreakerRegistry) {
        this.backendRegistry = backendRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
    }

    /**
     * Selects an optimal backend node for an incoming request and atomically increments
     * its in-flight connection counter.
     *
     * <p><b>Cleanup requirement:</b> The caller MUST decrement the returned node's connections
     * in a reactive {@code doFinally} block on all exit paths (success, error, timeout, cancel).</p>
     *
     * @return an {@link Optional} containing the selected {@link ServerNode}, or empty if
     *         no healthy/non-tripped backends are available.
     */
    public Optional<ServerNode> selectNode() {
        return selectNode(Set.of());
    }

    /**
     * Same as {@link #selectNode()}, but never returns a node whose id is in {@code excludedIds}.
     *
     * <p>Used by the proxy when retrying: the node that just failed is removed from this
     * request's candidate pool. The exclusion only narrows the pool, so the algorithm itself
     * (WLC, caps, WRR overflow) is unchanged.</p>
     */
    public Optional<ServerNode> selectNode(Set<String> excludedIds) {
        while (true) {
            List<ServerNode> healthy = getEligibleNodes();
            List<ServerNode> eligible = withoutExcluded(healthy, excludedIds);
            if (eligible.isEmpty()) {
                warnEmptyPoolRateLimited(excludedIds.isEmpty());
                return Optional.empty();
            }

            // Step 2: WLC Evaluation — filter nodes with remaining capacity
            List<ServerNode> underCap = new ArrayList<>();
            for (ServerNode node : eligible) {
                if (node.getActiveConnections() < node.getMaxCapacity()) {
                    underCap.add(node);
                }
            }

            // Step 3: If ALL eligible servers are at or over capacity, trigger Smooth WRR overflow
            if (underCap.isEmpty()) {
                ServerNode overflowNode = selectBySmoothWrr(eligible, healthy);
                log.debug("WLC capacity exhausted on all eligible nodes. Overflow routed to {} via Smooth WRR",
                        overflowNode.getId());
                return Optional.of(overflowNode);
            }

            // Pick best WLC candidate (lowest score = activeConnections / weight, tie break by highest weight)
            ServerNode candidate = selectWlcCandidate(underCap);

            boolean isHalfOpen = circuitBreakerRegistry.getState(candidate.getId()) == com.loadbalancer.resilience.CircuitBreakerState.HALF_OPEN;
            if (isHalfOpen && !circuitBreakerRegistry.tryAcquireProbe(candidate.getId())) {
                continue; // probe permit already claimed, re-evaluate
            }

            // Reserve the slot atomically via CAS check-and-increment
            if (candidate.tryReserveSlot()) {
                // SLF4J only understands "{}", so the score is formatted up front; guarded to keep the hot path free of String.format.
                if (log.isDebugEnabled()) {
                    log.debug("Routed to {} via WLC (score={}, active={})", candidate.getId(),
                            String.format("%.3f", candidate.wlcScore()), candidate.getActiveConnections());
                }
                return Optional.of(candidate);
            }

            if (isHalfOpen) {
                circuitBreakerRegistry.releaseProbePermit(candidate.getId());
            }

            // CAS failed because another thread updated connections.
            // Re-evaluate the entire eligible pool because relative scores may have shifted.
        }
    }

    /**
     * Selects an optimal backend node or throws {@link NoAvailableBackendException} (HTTP 503).
     */
    public ServerNode selectNodeOrThrow() {
        return selectNode().orElseThrow(NoAvailableBackendException::new);
    }

    /**
     * Returns all registered nodes that are healthy and whose circuit breakers can accept traffic.
     */
    public List<ServerNode> getEligibleNodes() {
        List<ServerNode> eligible = new ArrayList<>();
        for (ServerNode node : backendRegistry.allNodes()) {
            if (node.isHealthy() && circuitBreakerRegistry.canAcceptTraffic(node.getId())) {
                eligible.add(node);
            }
        }
        return eligible;
    }

    /**
     * Whether at least one healthy, non-tripped node exists outside {@code excludedIds}.
     * Lets the proxy check that a retry has somewhere to go before it spends retry budget.
     */
    public boolean hasAlternative(Set<String> excludedIds) {
        for (ServerNode node : getEligibleNodes()) {
            if (!excludedIds.contains(node.getId())) {
                return true;
            }
        }
        return false;
    }

    private static List<ServerNode> withoutExcluded(List<ServerNode> nodes, Set<String> excludedIds) {
        if (excludedIds.isEmpty()) {
            return nodes;
        }
        List<ServerNode> remaining = new ArrayList<>(nodes.size());
        for (ServerNode node : nodes) {
            if (!excludedIds.contains(node.getId())) {
                remaining.add(node);
            }
        }
        return remaining;
    }

    /**
     * Logs "no eligible backend" at most once per {@link #EMPTY_POOL_WARN_INTERVAL_MS}.
     * An empty pool after retry exclusions is expected, so it is never warned about.
     */
    private void warnEmptyPoolRateLimited(boolean noExclusions) {
        if (!noExclusions) {
            return;
        }
        long now = System.currentTimeMillis();
        long last = lastEmptyPoolWarnMs.get();
        if (now - last >= EMPTY_POOL_WARN_INTERVAL_MS && lastEmptyPoolWarnMs.compareAndSet(last, now)) {
            log.warn("No eligible backend nodes available (pool is empty, unhealthy, or circuit tripped)");
        }
    }

    /**
     * Evaluates candidates under max capacity:
     * <ul>
     *   <li>Lowest {@code activeConnections / weight} score wins.</li>
     *   <li>Ties are broken by highest {@code weight}.</li>
     *   <li>Secondary ties are broken deterministically by {@code id}.</li>
     * </ul>
     */
    private ServerNode selectWlcCandidate(List<ServerNode> candidates) {
        ServerNode best = candidates.get(0);
        double bestScore = best.wlcScore();

        for (int i = 1; i < candidates.size(); i++) {
            ServerNode cur = candidates.get(i);
            double curScore = cur.wlcScore();

            if (curScore < bestScore) {
                best = cur;
                bestScore = curScore;
            } else if (Double.compare(curScore, bestScore) == 0) {
                // Tie break by highest weight
                if (cur.getWeight() > best.getWeight()) {
                    best = cur;
                    bestScore = curScore;
                } else if (cur.getWeight() == best.getWeight()) {
                    // Secondary tie break: deterministic alphabetical ID order
                    if (cur.getId().compareTo(best.getId()) < 0) {
                        best = cur;
                        bestScore = curScore;
                    }
                }
            }
        }
        return best;
    }

    /**
     * Smooth Weighted Round Robin (nginx algorithm) across the eligible pool.
     *
     * <p>Algorithm per selection:
     * <ol>
     *   <li>For each node, {@code currentWeight += weight}.</li>
     *   <li>Select node with max {@code currentWeight}.</li>
     *   <li>Subtract sum of all weights from the chosen node's {@code currentWeight}.</li>
     *   <li>Increment the chosen node's in-flight connection counter.</li>
     * </ol>
     * </p>
     *
     * @param eligible candidates for this request (healthy, breaker not open, not excluded by a retry)
     * @param healthy  every node that is healthy with a non-open breaker; nodes outside this set
     *                 have their WRR state reset so they do not return with a stale balance
     */
    private ServerNode selectBySmoothWrr(List<ServerNode> eligible, List<ServerNode> healthy) {
        wrrLock.lock();
        try {
            // A node that left the pool (unhealthy / breaker open) keeps its old currentWeight.
            // When it returns that stale balance would give it a burst of traffic, so start it from zero.
            // Uses `healthy`, not `eligible`, so a per-request retry exclusion never disturbs WRR state.
            for (ServerNode node : backendRegistry.allNodes()) {
                if (!healthy.contains(node)) {
                    node.setCurrentWeight(0);
                }
            }

            int totalWeight = 0;
            ServerNode bestNode = null;
            int maxWeight = Integer.MIN_VALUE;

            for (ServerNode node : eligible) {
                totalWeight += node.getWeight();
                int updatedWeight = node.getCurrentWeight() + node.getWeight();
                node.setCurrentWeight(updatedWeight);

                if (bestNode == null || updatedWeight > maxWeight) {
                    maxWeight = updatedWeight;
                    bestNode = node;
                } else if (updatedWeight == maxWeight) {
                    // Tie break by higher configured weight, then id
                    if (node.getWeight() > bestNode.getWeight() ||
                            (node.getWeight() == bestNode.getWeight() && node.getId().compareTo(bestNode.getId()) < 0)) {
                        bestNode = node;
                    }
                }
            }

            if (bestNode != null) {
                bestNode.setCurrentWeight(bestNode.getCurrentWeight() - totalWeight);
                bestNode.incrementConnections();
                return bestNode;
            }

            // Fallback (unreachable if eligible is non-empty)
            ServerNode fallback = eligible.get(0);
            fallback.incrementConnections();
            return fallback;
        } finally {
            wrrLock.unlock();
        }
    }
}
