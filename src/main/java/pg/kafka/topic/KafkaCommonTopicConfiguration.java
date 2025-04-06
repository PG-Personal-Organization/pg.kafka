package pg.kafka.topic;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import pg.kafka.config.KafkaProperties;
import pg.kafka.config.KafkaPropertiesProvider;

@Log4j2
@Configuration
public class KafkaCommonTopicConfiguration {

    @Bean
    public KafkaAdmin.NewTopics topics(final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider) {
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();
        final var topicConfigs = kafkaProperties.getTopics();
        log.debug("Configuring topics started based on config: {}", topicConfigs);

        NewTopic[] newTopics = topicConfigs.stream()
                .map(topicConfig -> {
                    log.debug("Creating topic {} with definition: {}", topicConfig.getTopic().getName(), topicConfig);
                    return new NewTopic(
                            topicConfig.getTopic().getName(),
                            topicConfig.getPartitions(),
                            (short) topicConfig.getReplicationFactor()
                    );
                })
                .toArray(NewTopic[]::new);

        return new KafkaAdmin.NewTopics(newTopics);
    }
}
