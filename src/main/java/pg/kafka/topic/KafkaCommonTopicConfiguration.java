package pg.kafka.topic;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import pg.kafka.config.KafkaProperties;
import pg.kafka.config.KafkaPropertiesProvider;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j2
@Configuration
public class KafkaCommonTopicConfiguration {

    @Bean
    public KafkaAdmin.NewTopics topics(final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider, final @NonNull List<TopicDefinition> topicDefinitions) {
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();
        final var topicConfigs = kafkaProperties.getTopics();
        final var topicNames = topicConfigs.stream().map(TopicConfig::getTopic).toList();
        log.debug("Configuring topics started based on config: {}", topicConfigs);

        Set<NewTopic> newTopics = topicConfigs.stream()
                .map(topicConfig -> {
                    log.debug("Creating topic {} with config: {}", topicConfig.getTopic().getName(), topicConfig);
                    return new NewTopic(
                            topicConfig.getTopic().getName(),
                            topicConfig.getPartitions(),
                            (short) topicConfig.getReplicationFactor()
                    );
                })
                .collect(Collectors.toSet());

        for (TopicDefinition topic : topicDefinitions) {
            if (topicNames.contains(topic.getTopic())) {
                break;
            }
            log.debug("Creating topic {} with definition: {}", topic.getTopic(), topic);
            newTopics.add(new NewTopic(topic.getTopic().getName(), topic.getPartitions(), (short) topic.getReplicationFactor()));
        }

        log.debug("Configuring topics finished. Created topics: {}", newTopics);
        return new KafkaAdmin.NewTopics(newTopics.toArray(NewTopic[]::new));
    }
}
