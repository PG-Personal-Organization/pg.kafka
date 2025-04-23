package pg.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import pg.kafka.common.Commons;
import pg.kafka.consumer.ConsumerConfig;
import pg.kafka.consumer.KafkaCommonConsumerConfiguration;
import pg.kafka.producer.KafkaCommonProducerConfiguration;
import pg.kafka.producer.ProducerConfig;
import pg.kafka.topic.KafkaCommonTopicConfiguration;

import java.util.List;


@Log4j2
@ConditionalOnProperty(value = "pg.kafka.enabled", havingValue = "true")
@EnableKafka
@Configuration
@Import({
        KafkaCommonConfiguration.class,
        KafkaCommonTopicConfiguration.class,
        KafkaCommonProducerConfiguration.class,
        KafkaCommonConsumerConfiguration.class
})
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaConfiguration {
    private final KafkaProperties kafkaProperties;

    @Bean
    public KafkaPropertiesProvider kafkaPropertiesProvider() {
        log.info("Kafka properties: {}", kafkaProperties);
        return new KafkaPropertiesProvider(kafkaProperties);
    }

    @Bean
    public KafkaConfigurationProvider kafkaConfigurationProvider(final List<ProducerConfig> producerConfigs, final List<ConsumerConfig> consumerConfigs) {
        return new KafkaConfigurationProvider(producerConfigs, consumerConfigs);
    }

    @Bean(name = "kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        return Commons.defaultObjectMapper();
    }

}
