package com.loadbalancer.proxy;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.resilience.CircuitBreakerRegistry;
import com.loadbalancer.resilience.RetryBudget;
import com.loadbalancer.routing.BackendRegistry;
import com.loadbalancer.routing.SelectionEngine;
import com.loadbalancer.routing.ServerNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end tests for {@link ReverseProxyHandler}:
 * <ul>
 *   <li>HTTP GET/POST proxying and body streaming</li>
 *   <li>X-Forwarded-* header injection and hop-by-hop header stripping</li>
 *   <li>Guaranteed in-flight connection decrement on success and error</li>
 *   <li>Idempotent failover/retry to alternative backend when first backend fails</li>
 *   <li>503 Service Unavailable when no backends are available</li>
 * </ul>
 */
class ReverseProxyTest {

    private LoadBalancerProperties props;
    private BackendRegistry backendRegistry;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private SelectionEngine selectionEngine;
    private RetryBudget retryBudget;
    private ReverseProxyHandler proxyHandler;
    private WebTestClient client;

    private DisposableServer backendServer1;
    private DisposableServer backendServer2;

    @BeforeEach
    void setUp() {
        props = new LoadBalancerProperties();
        props.setCapacityPerWeightUnit(100);
        props.setConnectTimeoutMs(1000);
        props.setResponseTimeoutMs(2000);
        props.setMaxRetries(2);

        backendRegistry = new BackendRegistry(props);
        circuitBreakerRegistry = new CircuitBreakerRegistry();
        selectionEngine = new SelectionEngine(backendRegistry, circuitBreakerRegistry);
        retryBudget = new RetryBudget(props);

        ProxyClientConfig clientConfig = new ProxyClientConfig();
        proxyHandler = new ReverseProxyHandler(selectionEngine, clientConfig.proxyWebClient(props), retryBudget, circuitBreakerRegistry, props);

        ProxyRouterConfig routerConfig = new ProxyRouterConfig();
        client = WebTestClient.bindToRouterFunction(routerConfig.proxyRoutes(proxyHandler)).build();
    }

    @AfterEach
    void tearDown() {
        if (backendServer1 != null) {
            backendServer1.disposeNow();
        }
        if (backendServer2 != null) {
            backendServer2.disposeNow();
        }
    }

    @Test
    @DisplayName("proxies GET request, streams response body, and decrements in-flight counter to 0")
    void proxiesGetRequestAndCleansUp() {
        backendServer1 = HttpServer.create()
                .port(0)
                .route(routes -> routes.get("/api/hello", (req, res) ->
                        res.header("X-Custom-Header", "test-val")
                           .sendString(Mono.just("Hello from Backend 1"))))
                .bindNow();

        ServerNode node = backendRegistry.register("b1", "http://localhost:" + backendServer1.port(), 5, 0);
        assertEquals(0, node.getActiveConnections());

        client.get()
                .uri("/api/hello")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Custom-Header", "test-val")
                .expectBody(String.class).isEqualTo("Hello from Backend 1");

        // Counter must be 0 after completion
        assertEquals(0, node.getActiveConnections(), "In-flight connection counter must decrement to 0");
    }

    @Test
    @DisplayName("injects X-Forwarded-* headers and filters hop-by-hop headers")
    void injectsForwardedHeadersAndFiltersHopByHop() {
        AtomicReference<String> receivedXff = new AtomicReference<>();
        AtomicReference<String> receivedHost = new AtomicReference<>();
        AtomicReference<String> receivedProto = new AtomicReference<>();
        AtomicBoolean connectionHeaderFiltered = new AtomicBoolean(false);

        backendServer1 = HttpServer.create()
                .port(0)
                .route(routes -> routes.get("/inspect", (req, res) -> {
                    receivedXff.set(req.requestHeaders().get("X-Forwarded-For"));
                    receivedHost.set(req.requestHeaders().get("X-Forwarded-Host"));
                    receivedProto.set(req.requestHeaders().get("X-Forwarded-Proto"));
                    connectionHeaderFiltered.set(req.requestHeaders().get("Connection") == null ||
                            !req.requestHeaders().get("Connection").equalsIgnoreCase("close"));
                    return res.sendString(Mono.just("ok"));
                }))
                .bindNow();

        backendRegistry.register("b1", "http://localhost:" + backendServer1.port(), 1, 0);

        client.get()
                .uri("/inspect")
                .header(HttpHeaders.HOST, "my-gateway.example.com")
                .header("Connection", "close")
                .exchange()
                .expectStatus().isOk();

        assertNotNull(receivedXff.get());
        assertEquals("my-gateway.example.com", receivedHost.get());
        assertEquals("http", receivedProto.get());
        assertTrue(connectionHeaderFiltered.get());
    }

    @Test
    @DisplayName("proxies POST request with streaming body")
    void proxiesPostWithBody() {
        AtomicReference<String> receivedBody = new AtomicReference<>();

        backendServer1 = HttpServer.create()
                .port(0)
                .route(routes -> routes.post("/api/echo", (req, res) ->
                        res.send(req.receive().retain().doOnNext(buf -> {
                            byte[] bytes = new byte[buf.readableBytes()];
                            buf.getBytes(buf.readerIndex(), bytes);
                            receivedBody.set(new String(bytes));
                        }))))
                .bindNow();

        ServerNode node = backendRegistry.register("b1", "http://localhost:" + backendServer1.port(), 1, 0);

        client.post()
                .uri("/api/echo")
                .bodyValue("Payload Data")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Payload Data");

        assertEquals("Payload Data", receivedBody.get());
        assertEquals(0, node.getActiveConnections());
    }

    @Test
    @DisplayName("retries idempotent GET request on alternative backend when first backend returns 502")
    void retriesIdempotentRequestOnAlternativeNode() {
        // Backend 1 returns 502 Bad Gateway
        backendServer1 = HttpServer.create()
                .port(0)
                .route(routes -> routes.get("/flaky", (req, res) ->
                        res.status(502).sendString(Mono.just("Bad Gateway from B1"))))
                .bindNow();

        // Backend 2 returns 200 OK
        backendServer2 = HttpServer.create()
                .port(0)
                .route(routes -> routes.get("/flaky", (req, res) ->
                        res.sendString(Mono.just("Recovered on B2"))))
                .bindNow();

        ServerNode node1 = backendRegistry.register("b1", "http://localhost:" + backendServer1.port(), 5, 0);
        ServerNode node2 = backendRegistry.register("b2", "http://localhost:" + backendServer2.port(), 1, 0);

        // b1 has higher weight, so initial selection picks b1.
        // b1 returns 502, proxy retries on b2!
        client.get()
                .uri("/flaky")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Recovered on B2");

        // Both nodes must have 0 in-flight connections after completion
        assertEquals(0, node1.getActiveConnections(), "Failed node connections must be decremented");
        assertEquals(0, node2.getActiveConnections(), "Recovered node connections must be decremented");
    }

    @Test
    @DisplayName("returns 503 Service Unavailable when no healthy backends exist")
    void returns503WhenNoBackends() {
        client.get()
                .uri("/any-path")
                .exchange()
                .expectStatus().is5xxServerError()
                .expectStatus().isEqualTo(503);
    }
}
