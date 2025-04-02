package pg.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import pg.kafka.topic.TopicConfig;
import pg.kafka.topic.TopicName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "pg.kafka")
@Configuration
@Data
public class KafkaProperties {
    private String bootstrapServer;

    private List<TopicConfig> topics = new ArrayList<>();

    /**
     * {@link org.apache.kafka.clients.admin.AdminClientConfig} as key.
     * */
    private Map<String, Object> adminConfig = new HashMap<>();

    /**
     * {@link org.apache.kafka.clients.producer.ProducerConfig} as key of internal map.
     * */
    private Map<TopicName, Map<String, Object>> producerConfigs = new HashMap<>();

    /**
    * {@link org.apache.kafka.clients.consumer.ConsumerConfig} as key of internal map.
    * */
    private Map<TopicName, Map<String, Object>> consumerConfigs = new HashMap<>();

}
