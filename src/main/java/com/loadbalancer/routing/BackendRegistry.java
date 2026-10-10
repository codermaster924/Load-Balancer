package com.loadbalancer.routing;

import com.loadbalancer.config.LoadBalancerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry of all backend {@link ServerNode} instances.
 *
 * <p>Acts as the single source of truth for the backend fleet. All subsystems
 * (selection engine, health checker, metrics endpoint) read from this registry.
 * Write operations (registration) are rare and handled by the control-plane handler.</p>
 *
 * <p>Uses {@link ConcurrentHashMap} for O(1) lock-free reads; the map reference
 * itself is never replaced, so iterators obtained for selection remain stable.</p>
 */
@Component
public class BackendRegistry {

    private static final Logger log = LoggerFactory.getLogger(BackendRegistry.class);

    /** Node id → ServerNode. ConcurrentHashMap: lock-free reads, stripe-locked writes. */
    private final ConcurrentHashMap<String, ServerNode> nodes = new ConcurrentHashMap<>();

    private final LoadBalancerProperties props;

    public BackendRegistry(LoadBalancerProperties props) {
        this.props = props;
    }

    /**
     * Registers a new backend node, computing {@code maxCapacity} from weight when it
     * is not supplied, and validating the equity ratio when it is.
     *
     * <p>If a node with the same id already exists it is replaced atomically.</p>
     *
     * @param id          unique node identifier
     * @param url         base URL (no trailing slash)
     * @param weight      relative computational power (must be &gt; 0)
     * @param maxCapacity explicit cap, or ≤ 0 to derive from weight × K
     * @throws IllegalArgumentException if weight ≤ 0 or the supplied maxCapacity
     *                                  deviates too far from the configured K ratio
     */
    public ServerNode register(String id, String url, int weight, int maxCapacity) {
        if (weight <= 0) {
            throw new IllegalArgumentException("weight must be > 0 for node: " + id);
        }

        int k = props.getCapacityPerWeightUnit(); // K constant from application.yml

        if (maxCapacity <= 0) {
            // Derive from weight when caller did not supply it
            maxCapacity = weight * k;
        } else {
            // Validate that the caller-supplied ratio does not break equity
            double ratio = (double) maxCapacity / weight;
            double deviation = Math.abs(ratio - k) / k;
            if (deviation > props.getMaxCapacityDeviationFraction()) {
                throw new IllegalArgumentException(String.format(
                        "maxCapacity=%d deviates %.0f%% from weight×K (%d×%d=%d); " +
                        "allowed deviation is %.0f%%",
                        maxCapacity, deviation * 100,
                        weight, k, weight * k,
                        props.getMaxCapacityDeviationFraction() * 100));
            }
        }

        ServerNode node = new ServerNode(id, url, weight, maxCapacity);
        ServerNode previous = nodes.put(id, node);
        if (previous != null) {
            log.warn("Replaced existing node '{}' (url={}) with new registration (url={})",
                     id, previous.getUrl(), url);
        } else {
            log.info("Registered new backend node: {}", node);
        }
        return node;
    }

    /**
     * Returns an unmodifiable view of all registered nodes.
     * Callers must not cache the collection; call this method on each request
     * to see node additions made after their previous call.
     */
    public Collection<ServerNode> allNodes() {
        return Collections.unmodifiableCollection(nodes.values());
    }

    /** Looks up a node by id; returns empty if not found. */
    public Optional<ServerNode> findById(String id) {
        return Optional.ofNullable(nodes.get(id));
    }

    /** Total number of registered nodes (healthy or not). */
    public int size() {
        return nodes.size();
    }

    /** Number of nodes currently reported healthy by the health checker. */
    public long healthyCount() {
        return nodes.values().stream().filter(ServerNode::isHealthy).count();
    }
}
