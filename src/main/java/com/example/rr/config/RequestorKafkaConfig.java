package com.example.rr.config;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.TopicPartitionOffset;

@Configuration(proxyBeanMethods = false)
@Profile("requestor")
public class RequestorKafkaConfig {

    /**
     * Manually assigned to ONE reply partition (no group management => no rebalances on the reply side).
     * Because the container has exactly one explicit partition, ReplyingKafkaTemplate stamps
     * REPLY_TOPIC and REPLY_PARTITION headers on every request, so the processor replies only to us.
     * Seeks to END on start: replies produced while this instance was down belong to dead requests.
     */
    @Bean
    KafkaMessageListenerContainer<String, String> replyContainer(
            ConsumerFactory<String, String> consumerFactory, AppProperties props) {
        var tpo = new TopicPartitionOffset(
                props.topics().reply(), props.replyPartition(), TopicPartitionOffset.SeekPosition.END);
        var containerProps = new ContainerProperties(tpo);
        containerProps.setGroupId("requestor-" + props.instanceId());
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProps);
    }

    @Bean
    ReplyingKafkaTemplate<String, String, String> replyingTemplate(
            ProducerFactory<String, String> producerFactory,
            KafkaMessageListenerContainer<String, String> replyContainer,
            AppProperties props) {
        var template = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        // Spring Kafka's default is 5s – far beyond a 1000ms SLA.
        template.setDefaultReplyTimeout(Duration.ofMillis(props.replyTimeoutMs()));
        template.setSharedReplyTopic(false);
        return template;
    }
}
