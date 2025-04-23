package pg.kafka.config;

import lombok.NonNull;
import pg.kafka.consumer.ConsumerConfig;
import pg.kafka.producer.ProducerConfig;
import pg.kafka.topic.TopicName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KafkaConfigurationProvider {
    private final Map<TopicName, ProducerConfig> producerConfigs = new java.util.HashMap<>();
    private final Map</* Consumer name */String, ConsumerConfig> consumerConfigs = new java.util.HashMap<>();

    public KafkaConfigurationProvider(final List<ProducerConfig> producerConfigurations, final List<ConsumerConfig> consumerConfigurations) {
        producerConfigurations.forEach(producerConfig -> producerConfigs.put(producerConfig.getTopicName(), producerConfig));
        consumerConfigurations.forEach(consumerConfig -> consumerConfigs.put(consumerConfig.getConsumerName(), consumerConfig));
    }

    public Optional<ProducerConfig> getProducerConfig(final @NonNull TopicName topic) {
        return Optional.ofNullable(producerConfigs.get(topic));
    }
    public Optional<ConsumerConfig> getConsumerConfig(final @NonNull String consumerName) {
        return Optional.ofNullable(consumerConfigs.get(consumerName));
    }
}
