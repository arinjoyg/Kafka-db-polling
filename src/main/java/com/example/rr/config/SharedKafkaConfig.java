package com.example.rr.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration(proxyBeanMethods = false)
public class SharedKafkaConfig {

    /**
     * Declared explicitly because a ReplyingKafkaTemplate bean (requestor profile) IS a KafkaTemplate,
     * which would make Spring Boot's auto-configured KafkaTemplate back off. The processor's
     * {@code @SendTo} replies must go through this plain template, not the replying one.
     */
    @Bean
    @Primary
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    NewTopic requestTopic(AppProperties props) {
        return TopicBuilder.name(props.topics().request())
                .partitions(props.topics().requestPartitions())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    /** Same partition count as the request topic: the recoverer keeps the source partition. */
    @Bean
    NewTopic requestDeadLetterTopic(AppProperties props) {
        return TopicBuilder.name(props.topics().request() + ".DLT")
                .partitions(props.topics().requestPartitions())
                .replicas(props.topics().replicationFactor())
                .build();
    }

    @Bean
    NewTopic replyTopic(AppProperties props) {
        return TopicBuilder.name(props.topics().reply())
                .partitions(props.topics().replyPartitions())
                .replicas(props.topics().replicationFactor())
                // Replies are useless after the SLA; keep them short-lived.
                .config("retention.ms", "600000")
                .build();
    }
}
