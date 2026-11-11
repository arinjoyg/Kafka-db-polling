package com.example.rr.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@Profile("processor")
public class ProcessorKafkaConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> processorFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate,
            AppProperties props) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        // Concurrency <= request partitions; extra threads sit idle.
        factory.setConcurrency(props.processor().concurrency());
        // Used by @SendTo to publish the reply (to the REPLY_TOPIC / REPLY_PARTITION headers).
        factory.setReplyTemplate(kafkaTemplate);

        // At-least-once: offsets are committed after the listener (DB commit + reply send) returns.
        // BATCH keeps commit overhead off the per-record path; redelivery after a crash/rebalance is
        // absorbed by the idempotent upsert (processed_requests PK) and requestor-side correlation.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        // Retries must be short: anything retried past the deadline is wasted work.
        // The listener re-checks the deadline on every attempt.
        var errorHandler = new DefaultErrorHandler(new FixedBackOff(50L, 2L));
        errorHandler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
