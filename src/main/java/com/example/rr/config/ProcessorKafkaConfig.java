package com.example.rr.config;

import com.example.rr.processor.DbErrors;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@Profile("processor")
public class ProcessorKafkaConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> processorFactory(
            ConsumerFactory<String, String> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate,
            DefaultErrorHandler processorErrorHandler,
            AppProperties props) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.processor().concurrency());
        factory.setReplyTemplate(kafkaTemplate);
        // At-least-once; Kafka commits are advisory now: the DB-stored offset is authoritative after a switch.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setCommonErrorHandler(processorErrorHandler);
        return factory;
    }

    /**
     * Two very different failure classes:
     * <ul>
     *   <li><b>DB unavailable</b> (outage, switchover, fenced account): NEVER skip. Retry the same record with
     *       exponential backoff capped at 5s, indefinitely. Each retry is a fresh poll, so max.poll.interval.ms
     *       is never breached, and pausing the container still works. Skipping here is how messages get
     *       "processed in neither database".</li>
     *   <li><b>Bad record</b> (unparseable / invalid) or any other error: 2 quick retries, then publish to the
     *       DLT for investigation. Never silently dropped.</li>
     * </ul>
     */
    @Bean
    DefaultErrorHandler processorErrorHandler(KafkaTemplate<String, String> kafkaTemplate, AppProperties props) {
        String dlt = props.topics().request() + ".DLT";
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(dlt, record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(50L, 2L));
        handler.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        handler.setBackOffFunction((record, ex) -> DbErrors.isUnavailable(ex) ? dbUnavailableBackOff() : null);
        return handler;
    }

    private static BackOff dbUnavailableBackOff() {
        var backOff = new ExponentialBackOff(100L, 2.0);
        backOff.setMaxInterval(5_000L);
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }
}
