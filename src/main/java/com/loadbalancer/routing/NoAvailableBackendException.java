package com.loadbalancer.routing;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/**
 * Thrown when no healthy or non-tripped backend nodes are available to service a request.
 * Automatically maps to HTTP 503 Service Unavailable in Spring WebFlux.
 */
public class NoAvailableBackendException extends ResponseStatusException {

    public NoAvailableBackendException() {
        this("No available healthy backend nodes found");
    }

    public NoAvailableBackendException(String message) {
        super(HttpStatus.SERVICE_UNAVAILABLE, message);
    }
}
