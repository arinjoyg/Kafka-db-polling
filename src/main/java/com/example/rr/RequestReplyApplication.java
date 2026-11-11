package com.example.rr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * One deployable, two roles selected by Spring profile:
 * <ul>
 *   <li>{@code requestor} – REST edge that sends a request on Kafka and waits (bounded) for the reply.</li>
 *   <li>{@code processor} – Kafka consumer that performs an idempotent DB upsert and replies.</li>
 * </ul>
 * Default (no profile set) runs both, which is convenient locally and in integration tests.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RequestReplyApplication {

    public static void main(String[] args) {
        SpringApplication.run(RequestReplyApplication.class, args);
    }
}
