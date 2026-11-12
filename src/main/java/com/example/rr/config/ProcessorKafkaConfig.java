package com.example.rr.config;

import com.example.rr.processor.DbErrors;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@Profile("processor")
public class ProcessorKafkaConfig {

    public static final String REPLY_TEMPLATE = "processorReplyTemplate";

    /**
     * Transactional producer used ONLY for responses (and DLT records) from the processor. Each record is handled
     * in its own Kafka transaction containing the response AND the consumer offset, so a crash after sending the
     * response but before committing leaves the response aborted (invisible to read_committed consumers).
     * Built here rather than exposed as a ProducerFactory bean, so Spring Boot's non-transactional producer
     * (used by the requestor) and its JDBC transaction manager stay as they are.
     */
    @Bean(REPLY_TEMPLATE)
    KafkaTemplate<String, String> processorReplyTemplate(ProducerFactory<String, String> producerFactory,
                                                         AppProperties props) {
        Map<String, Object> config = new HashMap<>(producerFactory.getConfigurationProperties());
        // initTransactions() / coordinator lookup can take longer than the 500ms we allow plain sends.
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        var transactional = new DefaultKafkaProducerFactory<String, String>(config);
        // Unique per instance: two pods sharing a transactional.id would fence each other.
        transactional.setTransactionIdPrefix("rr-tx-" + props.instanceId() + "-");
        return new KafkaTemplate<>(transactional);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> processorFactory(
            ConsumerFactory<String, String> consumerFactory,
            @Qualifier(REPLY_TEMPLATE) KafkaTemplate<String, String> replyTemplate,
            AppProperties props) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(props.processor().concurrency());
        // Kafka transaction per record: response + offset commit atomically.
        // Not a bean: a second TransactionManager bean would switch off Boot's JDBC transaction manager.
        factory.getContainerProperties().setKafkaAwareTransactionManager(
                new KafkaTransactionManager<>(replyTemplate.getProducerFactory()));
        factory.setAfterRollbackProcessor(afterRollbackProcessor(replyTemplate, props));
        return factory;
    }

    /**
     * With transactions, failures are handled after rollback:
     * <ul>
     *   <li><b>DB unavailable</b> (outage, switchover, fenced account): NEVER give up. Retry the same record with
     *       exponential backoff capped at 5s, indefinitely. Skipping here is how messages get "processed in neither
     *       database".</li>
     *   <li><b>Bad record</b> or any other error: 2 quick retries, then publish to the DLT and commit its offset in
     *       the same transaction. Never silently dropped.</li>
     * </ul>
     */
    private static DefaultAfterRollbackProcessor<String, String> afterRollbackProcessor(
            KafkaTemplate<String, String> replyTemplate, AppProperties props) {
        String dlt = props.topics().request() + ".DLT";
        var recoverer = new DeadLetterPublishingRecoverer(replyTemplate,
                (record, ex) -> new TopicPartition(dlt, record.partition()));
        var processor = new DefaultAfterRollbackProcessor<String, String>(
                recoverer, new FixedBackOff(50L, 2L), replyTemplate, true);
        processor.addNotRetryableExceptions(JsonProcessingException.class, IllegalArgumentException.class);
        processor.setBackOffFunction((record, ex) -> DbErrors.isUnavailable(ex) ? dbUnavailableBackOff() : null);
        return processor;
    }

    private static BackOff dbUnavailableBackOff() {
        var backOff = new ExponentialBackOff(100L, 2.0);
        backOff.setMaxInterval(5_000L);
        backOff.setMaxElapsedTime(Long.MAX_VALUE);
        return backOff;
    }
}
