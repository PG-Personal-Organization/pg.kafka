package pg.kafka.config;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Configuration
public class KafkaCommonConfiguration {
    @Value("${pg.kafka-admin.topic-operation-timeout:300}")
    private int topicOperationTimeout;

    @Value("${pg.kafka-admin.auto-create:true}")
    private boolean autoCreate;

    private final KafkaPropertiesProvider kafkaPropertiesProvider;

    @Bean
    public KafkaAdmin admin() {
        Map<String, Object> configs = new HashMap<>();
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();

        configs.put(BOOTSTRAP_SERVERS_CONFIG, Collections.singletonList(kafkaProperties.getBootstrapServer()));
        configs.putAll(kafkaProperties.getAdminConfig());

        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setOperationTimeout(topicOperationTimeout);
        admin.setAutoCreate(autoCreate);

        return admin;
    }
}
