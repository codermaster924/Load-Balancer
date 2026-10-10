package com.loadbalancer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the custom L7/L4 Load Balancer Gateway.
 *
 * <p>@EnableScheduling activates the background health-check worker (60 s interval)
 * without pulling in any Spring Cloud or Ribbon dependency.</p>
 */
@SpringBootApplication
@EnableScheduling
public class LoadBalancerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoadBalancerApplication.class, args);
    }
}
