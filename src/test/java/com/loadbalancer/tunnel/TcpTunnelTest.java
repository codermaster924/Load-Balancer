package com.loadbalancer.tunnel;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.resilience.CircuitBreakerRegistry;
import com.loadbalancer.routing.BackendRegistry;
import com.loadbalancer.routing.SelectionEngine;
import com.loadbalancer.routing.ServerNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TcpTunnelTest {

    private LoadBalancerProperties props;
    private BackendRegistry backendRegistry;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private SelectionEngine selectionEngine;
    private TcpTunnelServer tunnelServer;

    private DisposableServer backendTcpServer;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCapacityPerWeightUnit(100);
        props.setConnectTimeoutMs(2000);

        backendRegistry = new BackendRegistry(props);
        circuitBreakerRegistry = new CircuitBreakerRegistry();
        selectionEngine = new SelectionEngine(backendRegistry, circuitBreakerRegistry);

        tunnelServer = new TcpTunnelServer(selectionEngine, props);
    }

    @AfterEach
    void tearDown() {
        if (tunnelServer != null && tunnelServer.isRunning()) {
            tunnelServer.stop();
        }
        if (backendTcpServer != null) {
            backendTcpServer.disposeNow();
        }
    }

    @Test
    @DisplayName("proxies raw TCP payload bidirectionally and tracks active connection counts")
    void proxiesRawTcpBidirectionally() {
        // 1. Start echo backend server
        backendTcpServer = TcpServer.create()
                .port(0)
                .handle((in, out) -> out.send(in.receive().retain()))
                .bindNow();

        // 2. Register backend node in load balancer registry
        ServerNode node = backendRegistry.register("echo-backend",
                "http://localhost:" + backendTcpServer.port(), 5, 500);
        assertEquals(0, node.getActiveConnections());

        // 3. Start tunnel server on ephemeral port
        tunnelServer.start(0);
        int tunnelPort = tunnelServer.port();
        assertTrue(tunnelPort > 0);

        // 4. Connect client to tunnel and send payload
        AtomicReference<String> receivedPayload = new AtomicReference<>();

        Connection clientConn = TcpClient.create()
                .host("localhost")
                .port(tunnelPort)
                .connectNow();

        clientConn.inbound().receive().asString()
                .take(1)
                .doOnNext(receivedPayload::set)
                .subscribe();

        clientConn.outbound()
                .sendString(Mono.just("Ping Over L4 Tunnel\n"))
                .then()
                .block(Duration.ofSeconds(3));

        // Wait for echo reply
        long deadline = System.currentTimeMillis() + 3000;
        while (receivedPayload.get() == null && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {}
        }

        assertEquals("Ping Over L4 Tunnel\n", receivedPayload.get());

        // 5. Disconnect client and verify connection counter decrements to 0
        clientConn.disposeNow();

        deadline = System.currentTimeMillis() + 2000;
        while (node.getActiveConnections() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ignored) {}
        }

        assertEquals(0, node.getActiveConnections(), "In-flight connection count must decrement to 0 on disconnect");
    }

    @Test
    @DisplayName("correctly extracts SNI hostname from ClientHello packet")
    void extractsSniHostname() {
        // Construct a synthetic ClientHello packet with SNI 'gateway.test.local'
        String host = "gateway.test.local";
        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);

        ByteBuf buf = Unpooled.buffer();
        // Record Header
        buf.writeByte(0x16); // Handshake
        buf.writeShort(0x0303); // TLS 1.2
        int recordLenPos = buf.writerIndex();
        buf.writeShort(0); // placeholder for record length

        // Handshake Header
        int handshakeStart = buf.writerIndex();
        buf.writeByte(0x01); // ClientHello
        buf.writeMedium(0); // placeholder for handshake length
        buf.writeShort(0x0303); // client version
        buf.writeBytes(new byte[32]); // random (32 bytes)
        buf.writeByte(0); // session id length = 0

        // Cipher suites
        buf.writeShort(2); // cipher suites length = 2
        buf.writeShort(0x002F); // TLS_RSA_WITH_AES_128_CBC_SHA

        // Compression methods
        buf.writeByte(1); // compression length = 1
        buf.writeByte(0); // null compression

        // Extensions
        int extStart = buf.writerIndex();
        buf.writeShort(0); // placeholder for extensions length

        // Server Name extension (0x0000)
        buf.writeShort(0x0000); // extension type = server_name
        int extDataLenPos = buf.writerIndex();
        buf.writeShort(0); // placeholder for extension length

        int extDataStart = buf.writerIndex();
        buf.writeShort(hostBytes.length + 3); // server name list length
        buf.writeByte(0x00); // host_name
        buf.writeShort(hostBytes.length); // host name length
        buf.writeBytes(hostBytes);

        // Patch lengths
        int extDataLen = buf.writerIndex() - extDataStart;
        buf.setShort(extDataLenPos, extDataLen);

        int totalExtLen = buf.writerIndex() - (extStart + 2);
        buf.setShort(extStart, totalExtLen);

        int handshakeLen = buf.writerIndex() - (handshakeStart + 4);
        buf.setMedium(handshakeStart + 1, handshakeLen);

        int recordLen = buf.writerIndex() - 5;
        buf.setShort(recordLenPos, recordLen);

        byte[] clientHelloBytes = new byte[buf.readableBytes()];
        buf.readBytes(clientHelloBytes);
        buf.release();

        Optional<String> sni = SniParser.parseSni(clientHelloBytes);
        assertTrue(sni.isPresent(), "SNI parser must find server_name extension");
        assertEquals("gateway.test.local", sni.get());
    }

    @Test
    @DisplayName("closes inbound connection gracefully when no backends are available")
    void closesConnectionWhenNoBackendsAvailable() {
        tunnelServer.start(0);
        int tunnelPort = tunnelServer.port();

        // No backends registered in registry
        assertEquals(0, backendRegistry.size());

        // Connecting to tunnel should close immediately because no backends exist
        Connection conn = TcpClient.create()
                .host("localhost")
                .port(tunnelPort)
                .connectNow();

        // Reading inbound should complete immediately due to server-side closure
        conn.inbound().receive().then().block(Duration.ofSeconds(3));
        conn.disposeNow();
    }
}
