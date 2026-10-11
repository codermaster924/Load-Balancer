package com.loadbalancer.proxy;

import com.loadbalancer.config.LoadBalancerProperties;
import com.loadbalancer.resilience.CircuitBreakerRegistry;
import com.loadbalancer.resilience.RetryBudget;
import com.loadbalancer.routing.NoAvailableBackendException;
import com.loadbalancer.routing.SelectionEngine;
import com.loadbalancer.routing.ServerNode;
import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyExtractors;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Core Layer 7 Reverse Proxy handler.
 *
 * <p>Executes upstream forwarding using non-blocking {@link WebClient}:
 * <ul>
 *   <li>Filters hop-by-hop headers and propagates {@code X-Forwarded-*} headers.</li>
 *   <li>Evaluates node selection via {@link SelectionEngine}.</li>
 *   <li>Streams request and response bodies without buffering into memory.</li>
 *   <li><b>Mandatory in-flight cleanup:</b> Guarantees connection counter decrement
 *       in {@code doFinally} on every exit path (success, error, timeout, cancel).</li>
 *   <li><b>Idempotent retries:</b> Retries idempotent requests without body on connect
 *       failures or 502/503/504, capped by {@link RetryBudget} and {@code lb.max-retries}.</li>
 * </ul>
 * </p>
 */
@Component
public class ReverseProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(ReverseProxyHandler.class);

    private static final Set<String> HOP_BY_HOP_HEADERS = Set.of(
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "transfer-encoding",
            "upgrade"
    );

    private final SelectionEngine selectionEngine;
    private final WebClient webClient;
    private final RetryBudget retryBudget;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final LoadBalancerProperties props;

    public ReverseProxyHandler(SelectionEngine selectionEngine,
                               WebClient webClient,
                               RetryBudget retryBudget,
                               CircuitBreakerRegistry circuitBreakerRegistry,
                               LoadBalancerProperties props) {
        this.selectionEngine = selectionEngine;
        this.webClient = webClient;
        this.retryBudget = retryBudget;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.props = props;
    }

    /**
     * Entry point for incoming HTTP requests matching the proxy catch-all route.
     */
    public Mono<ServerResponse> handle(ServerRequest request) {
        retryBudget.recordRequest();

        boolean hasBody = hasRequestBody(request);
        Set<String> excludedIds = new HashSet<>();

        return forwardWithRetry(request, excludedIds, 0, hasBody);
    }

    /**
     * Recursive reactive forwarder that attempts selection, dispatch, and optional retry.
     */
    private Mono<ServerResponse> forwardWithRetry(ServerRequest request,
                                                  Set<String> excludedIds,
                                                  int attempt,
                                                  boolean hasBody) {
        // Step 1: Select backend node (excluding previously failed nodes for this request)
        Optional<ServerNode> maybeNode = selectionEngine.selectNode(excludedIds);
        if (maybeNode.isEmpty()) {
            if (attempt == 0) {
                log.warn("No healthy backends available for path: {}", request.path());
                return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .bodyValue("503 Service Unavailable: No healthy backends available");
            } else {
                // If retry ran out of alternative nodes, return 503
                return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .bodyValue("503 Service Unavailable: All failover candidates exhausted");
            }
        }

        ServerNode node = maybeNode.get();
        ConnectionReservation reservation = new ConnectionReservation(node);
        URI targetUri = buildTargetUri(node, request);

        HttpMethod method = request.method();
        boolean canRetry = isIdempotentMethod(method) && !hasBody && (attempt < props.getMaxRetries());

        // Step 2: Prepare upstream WebClient request
        WebClient.RequestBodySpec requestSpec = webClient
                .method(method)
                .uri(targetUri)
                .headers(headers -> copyRequestHeaders(request, headers, targetUri));

        if (hasBody) {
            requestSpec.body(BodyInserters.fromDataBuffers(request.body(BodyExtractors.toDataBuffers())));
        }

        // Step 3: Dispatch upstream and handle response or error
        @SuppressWarnings("deprecation")
        Mono<ServerResponse> responseMono = requestSpec.exchange()
                .flatMap(clientResponse -> handleUpstreamResponse(
                        request, clientResponse, reservation, excludedIds, attempt, hasBody, canRetry));

        return responseMono.onErrorResume(error -> handleUpstreamError(
                request, error, reservation, excludedIds, attempt, hasBody, canRetry));
    }

    /**
     * Handles upstream HTTP response headers from WebClient.
     */
    private Mono<ServerResponse> handleUpstreamResponse(ServerRequest originalRequest,
                                                        ClientResponse clientResponse,
                                                        ConnectionReservation reservation,
                                                        Set<String> excludedIds,
                                                        int attempt,
                                                        boolean hasBody,
                                                        boolean canRetry) {
        HttpStatusCode status = clientResponse.statusCode();

        // Record circuit breaker outcome: 5xx is an error; 2xx/3xx/4xx is a success
        if (status.is5xxServerError()) {
            circuitBreakerRegistry.recordError(reservation.getNode().getId());
        } else {
            circuitBreakerRegistry.recordSuccess(reservation.getNode().getId());
        }

        // Check if status is a retryable server error (502, 503, 504)
        if (isRetryableStatus(status) && canRetry && canAttemptRetry(reservation.getNode().getId(), excludedIds)) {
            log.warn("Upstream {} returned {}. Retrying on alternative node (attempt {}/{})",
                    reservation.getNode().getId(), status.value(), attempt + 1, props.getMaxRetries());

            reservation.release();
            excludedIds.add(reservation.getNode().getId());

            return clientResponse.releaseBody()
                    .then(forwardWithRetry(originalRequest, excludedIds, attempt + 1, hasBody));
        }

        // Normal response path: stream response body back to downstream client.
        // The in-flight connection reservation is released in doFinally when the body transfer finishes.
        Flux<DataBuffer> responseBody = clientResponse.body(BodyExtractors.toDataBuffers())
                .doFinally(signalType -> reservation.release());

        return ServerResponse.status(status)
                .headers(h -> copyResponseHeaders(clientResponse.headers().asHttpHeaders(), h))
                .body(BodyInserters.fromDataBuffers(responseBody));
    }

    /**
     * Handles network failures, connect timeouts, and response timeouts.
     */
    private Mono<ServerResponse> handleUpstreamError(ServerRequest originalRequest,
                                                     Throwable error,
                                                     ConnectionReservation reservation,
                                                     Set<String> excludedIds,
                                                     int attempt,
                                                     boolean hasBody,
                                                     boolean canRetry) {
        String failedNodeId = reservation.getNode().getId();
        reservation.release();
        circuitBreakerRegistry.recordError(failedNodeId);

        if (canRetry && canAttemptRetry(failedNodeId, excludedIds)) {
            log.warn("Upstream {} failed with '{}'. Retrying on alternative node (attempt {}/{})",
                    failedNodeId, error.getMessage(), attempt + 1, props.getMaxRetries());

            excludedIds.add(failedNodeId);
            return forwardWithRetry(originalRequest, excludedIds, attempt + 1, hasBody);
        }

        // Non-retryable error: map to appropriate gateway error status
        if (isTimeoutException(error)) {
            log.error("Gateway timeout proxying to {}: {}", failedNodeId, error.getMessage());
            return ServerResponse.status(HttpStatus.GATEWAY_TIMEOUT)
                    .bodyValue("504 Gateway Timeout: " + error.getMessage());
        }

        log.error("Bad gateway proxying to {}: {}", failedNodeId, error.getMessage());
        return ServerResponse.status(HttpStatus.BAD_GATEWAY)
                .bodyValue("502 Bad Gateway: " + error.getMessage());
    }

    /**
     * Verifies if retry budget and alternative nodes are available before retrying.
     */
    private boolean canAttemptRetry(String failedNodeId, Set<String> currentExclusions) {
        Set<String> nextExclusions = new HashSet<>(currentExclusions);
        nextExclusions.add(failedNodeId);

        if (!selectionEngine.hasAlternative(nextExclusions)) {
            return false;
        }
        return retryBudget.tryAcquire();
    }

    private URI buildTargetUri(ServerNode node, ServerRequest request) {
        String base = node.getUrl();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }

        String path = request.uri().getRawPath();
        String query = request.uri().getRawQuery();

        String target = base + path + (query != null && !query.isEmpty() ? "?" + query : "");
        return URI.create(target);
    }

    private void copyRequestHeaders(ServerRequest source, HttpHeaders target, URI targetUri) {
        source.headers().asHttpHeaders().forEach((name, values) -> {
            String lower = name.toLowerCase(Locale.ROOT);
            if (!HOP_BY_HOP_HEADERS.contains(lower) && !lower.equals("host")) {
                target.put(name, values);
            }
        });

        // Set Host to backend host:port
        target.set(HttpHeaders.HOST, targetUri.getAuthority());

        // Compute X-Forwarded-* headers
        applyForwardedHeaders(source, target);
    }

    private void applyForwardedHeaders(ServerRequest request, HttpHeaders target) {
        // X-Forwarded-For
        String clientIp = resolveClientIp(request);
        List<String> existingXff = request.headers().header("X-Forwarded-For");
        if (!existingXff.isEmpty()) {
            target.set("X-Forwarded-For", String.join(", ", existingXff) + ", " + clientIp);
        } else {
            target.set("X-Forwarded-For", clientIp);
        }

        // X-Forwarded-Host
        String host = request.headers().firstHeader(HttpHeaders.HOST);
        if (host != null && !host.isBlank()) {
            target.set("X-Forwarded-Host", host);
        } else {
            target.set("X-Forwarded-Host", request.uri().getAuthority());
        }

        // X-Forwarded-Proto
        String proto = request.uri().getScheme();
        target.set("X-Forwarded-Proto", proto != null ? proto : "http");

        // X-Forwarded-Port
        int port = request.uri().getPort();
        if (port > 0) {
            target.set("X-Forwarded-Port", String.valueOf(port));
        } else {
            target.set("X-Forwarded-Port", "https".equalsIgnoreCase(proto) ? "443" : "80");
        }
    }

    private void copyResponseHeaders(HttpHeaders source, HttpHeaders target) {
        source.forEach((name, values) -> {
            String lower = name.toLowerCase(Locale.ROOT);
            if (!HOP_BY_HOP_HEADERS.contains(lower)) {
                target.put(name, values);
            }
        });
    }

    private String resolveClientIp(ServerRequest request) {
        return request.remoteAddress()
                .map(InetSocketAddress::getAddress)
                .map(java.net.InetAddress::getHostAddress)
                .orElse("127.0.0.1");
    }

    private boolean hasRequestBody(ServerRequest request) {
        java.util.OptionalLong contentLength = request.headers().contentLength();
        if (contentLength.isPresent() && contentLength.getAsLong() > 0) {
            return true;
        }
        return request.headers().firstHeader(HttpHeaders.TRANSFER_ENCODING) != null;
    }

    private boolean isIdempotentMethod(HttpMethod method) {
        return method == HttpMethod.GET ||
                method == HttpMethod.HEAD ||
                method == HttpMethod.PUT ||
                method == HttpMethod.DELETE ||
                method == HttpMethod.OPTIONS ||
                method == HttpMethod.TRACE;
    }

    private boolean isRetryableStatus(HttpStatusCode status) {
        return status == HttpStatus.BAD_GATEWAY ||
                status == HttpStatus.SERVICE_UNAVAILABLE ||
                status == HttpStatus.GATEWAY_TIMEOUT;
    }

    private boolean isTimeoutException(Throwable error) {
        if (error instanceof TimeoutException ||
                error instanceof ReadTimeoutException ||
                error instanceof ConnectTimeoutException) {
            return true;
        }
        if (error instanceof WebClientRequestException wcre) {
            Throwable cause = wcre.getCause();
            return cause instanceof TimeoutException ||
                    cause instanceof ReadTimeoutException ||
                    cause instanceof ConnectTimeoutException;
        }
        return false;
    }
}
