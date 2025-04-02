package pg.kafka.producer;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import pg.kafka.common.Commons;
import pg.kafka.config.KafkaPropertiesProvider;
import pg.kafka.config.MessagesDestinationConfig;
import pg.kafka.message.Message;
import pg.kafka.message.MessageDestination;
import pg.kafka.sender.EventSender;
import pg.kafka.sender.KafkaEventSender;
import pg.kafka.topic.TopicName;
import pg.lib.common.spring.storage.HeadersHolder;

import java.util.HashMap;
import java.util.Map;

@Log4j2
@Configuration
public class KafkaCommonProducerConfiguration {

    @Bean
    public EventSender kafkaEventSender(final @NonNull DestinationResolver destinationResolver,
                                        final @NonNull HeadersHolder headersHolder) {
        return new KafkaEventSender(destinationResolver, headersHolder);
    }

    @Bean
    public DestinationResolver destinationResolver(final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider,
                                                   final @NonNull MessagesDestinationConfig messagesDestinationConfig) {
        var kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();

        Map<TopicName, KafkaTemplate<String, ? extends Message>> topicToTemplates = new HashMap<>();
        Map<Class<? extends Message>, TopicName> destinations = new HashMap<>();

        var messageDestinations = messagesDestinationConfig.getDestinations();
        var producerConfigs = kafkaProperties.getProducerConfigs();
        log.debug("Initializing destination resolver with following destinations: {} and producer configs: {}",
                messageDestinations, producerConfigs);

        for (MessageDestination destination : messageDestinations) {
            var template = buildTemplate(destination, producerConfigs);
            destinations.put(destination.getMessageClass(), destination.getTopic());
            topicToTemplates.put(destination.getTopic(), template);
        }

        var destinationResolver = new DestinationResolver(destinations, topicToTemplates);
        log.debug("Initialized destination resolver: {}", destinationResolver);
        return destinationResolver;
    }

    private <T extends Message> KafkaTemplate<String, T> buildTemplate(final MessageDestination destination,
                                                                       final Map<TopicName, Map<String, Object>> producerConfigs) {
        var producerConfig = Commons.defaultProducerProperties();
        producerConfig.putAll(producerConfigs.getOrDefault(destination.getTopic(), new HashMap<>()));
        log.debug("Creating producer for topic: {} with config: {}", destination.getTopic(), producerConfig);

        var producerFactory = new DefaultKafkaProducerFactory<String, T>(producerConfig);

        var template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic(destination.getTopic().getName());
        template.setAllowNonTransactional(true);
        return template;
    }

}
