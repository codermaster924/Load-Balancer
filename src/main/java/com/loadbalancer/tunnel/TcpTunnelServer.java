package com.loadbalancer.tunnel;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.proxy.ConnectionReservation;
import com.loadbalancer.routing.SelectionEngine;
import com.loadbalancer.routing.ServerNode;
import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Layer 4 TCP Tunnel / Fallback Proxy Server.
 *
 * <p>Implements specification Section 3.F &amp; API Contract Route 2:
 * <ul>
 *   <li>Runs on a dedicated TCP port (default {@code lb.tunnel-port} = 8443).</li>
 *   <li>Bypasses deep L7 inspection and TLS termination; forwards raw TCP/TLS bytes.</li>
 *   <li>Parses SNI hostname from {@code ClientHello} for logging and metrics without hostname-to-pool routing.</li>
 *   <li>Shares the same {@link SelectionEngine} and increments/decrements the same in-flight
 *       connections and capacity limits as L7 traffic.</li>
 *   <li>Guarantees connection decrement on channel termination in a reactive {@code doFinally} block.</li>
 * </ul>
 * </p>
 */
@Component
public class TcpTunnelServer {

    private static final Logger log = LoggerFactory.getLogger(TcpTunnelServer.class);

    private final SelectionEngine selectionEngine;
    private final LoadBalancerProperties props;
    private DisposableServer disposableServer;

    public TcpTunnelServer(SelectionEngine selectionEngine, LoadBalancerProperties props) {
        this.selectionEngine = selectionEngine;
        this.props = props;
    }

    /**
     * Binds and starts the TCP listener on the configured port.
     */
    public synchronized void start() {
        start(props.getTunnelPort());
    }

    /**
     * Binds and starts the TCP listener on an explicit port (used for tests with port 0).
     */
    public synchronized void start(int port) {
        if (disposableServer != null) {
            return;
        }

        log.info("Starting L4 TCP Tunnel listener on port {}...", port);

        this.disposableServer = TcpServer.create()
                .port(port)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) props.getConnectTimeoutMs())
                .handle((inbound, outbound) -> {
                    // Step 1: Select backend node (WLC / overflow)
                    Optional<ServerNode> maybeNode = selectionEngine.selectNode();
                    if (maybeNode.isEmpty()) {
                        log.warn("L4 Tunnel: No available backend nodes. Closing incoming connection.");
                        inbound.withConnection(conn -> conn.channel().close());
                        return Mono.empty();
                    }

                    ServerNode node = maybeNode.get();
                    ConnectionReservation reservation = new ConnectionReservation(node);

                    // Step 2: Resolve backend target host & port from node URL
                    URI uri = URI.create(node.getUrl());
                    String targetHost = uri.getHost() != null ? uri.getHost() : "localhost";
                    int targetPort = uri.getPort() != -1 ? uri.getPort() :
                            (uri.getScheme() != null && uri.getScheme().equalsIgnoreCase("https") ? 443 : 80);

                    AtomicBoolean sniLogged = new AtomicBoolean(false);

                    // Step 3: Connect to backend and duplex bridge the traffic
                    return TcpClient.create()
                            .host(targetHost)
                            .port(targetPort)
                            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) props.getConnectTimeoutMs())
                            .connect()
                            .flatMap(upstreamConn -> {
                                // Inbound client -> Upstream backend
                                Mono<Void> clientToUpstream = upstreamConn.outbound()
                                        .send(inbound.receive().retain().doOnNext(buf -> {
                                            if (sniLogged.compareAndSet(false, true)) {
                                                try {
                                                    byte[] sample = new byte[Math.min(buf.readableBytes(), 512)];
                                                    buf.getBytes(buf.readerIndex(), sample);
                                                    SniParser.parseSni(sample).ifPresent(sni ->
                                                            log.info("L4 Tunnel SNI hostname: '{}' -> routed to backend '{}'", sni, node.getId())
                                                    );
                                                } catch (Exception ignored) {}
                                            }
                                        }))
                                        .then();

                                // Upstream backend -> Inbound client
                                Mono<Void> upstreamToClient = outbound
                                        .send(upstreamConn.inbound().receive().retain())
                                        .then();

                                return Mono.zip(clientToUpstream, upstreamToClient)
                                        .then()
                                        .doFinally(signal -> upstreamConn.dispose());
                            })
                            .onErrorResume(error -> {
                                log.error("L4 Tunnel error bridging to {} ({}:{}): {}",
                                        node.getId(), targetHost, targetPort, error.getMessage());
                                return Mono.empty();
                            })
                            .doFinally(signal -> reservation.release());
                })
                .bindNow();

        log.info("L4 TCP Tunnel listener successfully bound to port {}", port());
    }

    /**
     * Gracefully stops the TCP server.
     */
    public synchronized void stop() {
        if (disposableServer != null) {
            log.info("Stopping L4 TCP Tunnel listener on port {}...", port());
            disposableServer.disposeNow();
            disposableServer = null;
        }
    }

    public boolean isRunning() {
        return disposableServer != null && !disposableServer.isDisposed();
    }

    public int port() {
        return disposableServer != null ? disposableServer.port() : -1;
    }
}
