package com.loadbalancer.proxy;

import com.loadbalancer.routing.ServerNode;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates an active in-flight connection reservation on a {@link ServerNode}.
 *
 * <p>Guarantees that {@link ServerNode#decrementConnections()} is invoked at most once
 * on any exit path (success, error, timeout, client cancel), preventing in-flight counter leaks.</p>
 */
public class ConnectionReservation {

    private final ServerNode node;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public ConnectionReservation(ServerNode node) {
        if (node == null) {
            throw new IllegalArgumentException("node cannot be null");
        }
        this.node = node;
    }

    public ServerNode getNode() {
        return node;
    }

    /**
     * Decrements the server node's in-flight connection counter exactly once.
     * Subsequent calls are safe no-ops.
     */
    public void release() {
        if (released.compareAndSet(false, true)) {
            node.decrementConnections();
        }
    }

    public boolean isReleased() {
        return released.get();
    }
}
