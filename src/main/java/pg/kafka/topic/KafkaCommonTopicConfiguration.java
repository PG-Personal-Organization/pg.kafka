package pg.kafka.topic;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pg.kafka.config.KafkaProperties;
import pg.kafka.config.KafkaPropertiesProvider;

import java.util.ArrayList;
import java.util.List;

@Log4j2
@Configuration
public class KafkaCommonTopicConfiguration {

    @Bean
    public List<NewTopic> topics(final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider) {
        List<NewTopic> topics = new ArrayList<>();
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();
        final var topicConfigs = kafkaProperties.getTopics();
        log.debug("Configuring topics started based on config: {}", topicConfigs);

        topicConfigs.forEach(topicConfig -> {
            NewTopic newTopic = new NewTopic(
                    topicConfig.getTopic().getName(),
                    topicConfig.getPartitions(),
                    (short) topicConfig.getReplicationFactor());
            log.debug("Creating topic {} with definition: {}", topicConfig.getTopic().getName(), topicConfig);
            topics.add(newTopic);
        });
        return topics;
    }
}
