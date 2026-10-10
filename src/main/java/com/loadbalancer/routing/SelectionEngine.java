package com.loadbalancer.routing;

import com.loadbalancer.resilience.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

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

    private final BackendRegistry backendRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    /**
     * Lightweight lock guarding the Smooth WRR state mutations during overflow.
     * Per specification, this path is rare, so a single lock avoids concurrency anomalies
     * in currentWeight calculations.
     */
    private final ReentrantLock wrrLock = new ReentrantLock();

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
        while (true) {
            List<ServerNode> eligible = getEligibleNodes();
            if (eligible.isEmpty()) {
                log.warn("No eligible backend nodes available (pool is empty, unhealthy, or circuit tripped)");
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
                ServerNode overflowNode = selectBySmoothWrr(eligible);
                log.debug("WLC capacity exhausted on all eligible nodes. Overflow routed to {} via Smooth WRR",
                        overflowNode.getId());
                return Optional.of(overflowNode);
            }

            // Pick best WLC candidate (lowest score = activeConnections / weight, tie break by highest weight)
            ServerNode candidate = selectWlcCandidate(underCap);

            // Reserve the slot atomically via CAS check-and-increment
            if (candidate.tryReserveSlot()) {
                log.debug("Routed to {} via WLC (score={:.3f}, active={})",
                        candidate.getId(), candidate.wlcScore(), candidate.getActiveConnections());
                return Optional.of(candidate);
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
     * Returns all registered nodes that are healthy and whose circuit breakers are not open.
     */
    public List<ServerNode> getEligibleNodes() {
        List<ServerNode> eligible = new ArrayList<>();
        for (ServerNode node : backendRegistry.allNodes()) {
            if (node.isHealthy() && !circuitBreakerRegistry.isOpen(node.getId())) {
                eligible.add(node);
            }
        }
        return eligible;
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
     */
    private ServerNode selectBySmoothWrr(List<ServerNode> eligible) {
        wrrLock.lock();
        try {
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
