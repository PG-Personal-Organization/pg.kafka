package pg.kafka.producer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import pg.kafka.message.Message;
import pg.kafka.producer.exceptions.DestinationNotFoundException;
import pg.kafka.producer.exceptions.KafkaTemplateNotFoundException;
import pg.kafka.topic.TopicName;

import java.util.Map;
import java.util.Optional;

@Log4j2
@RequiredArgsConstructor
@ToString
public class DestinationResolver {
    private final Map<Class<? extends Message>, TopicName> destinations;
    private final Map<TopicName, KafkaTemplate<String, ? extends Message>> kafkaTemplates;

    public TopicName resolveDestination(final Class<? extends Message> messageClass) throws DestinationNotFoundException {
        return Optional.ofNullable(destinations.get(messageClass))
                .orElseThrow(() -> new DestinationNotFoundException(messageClass));
    }

    @SuppressWarnings("unchecked")
    public <T extends Message> KafkaTemplate<String, T> getTemplate(final @NonNull TopicName topic) throws KafkaTemplateNotFoundException {
        return (KafkaTemplate<String, T>) Optional.ofNullable(kafkaTemplates.get(topic))
                .orElseThrow(() -> new KafkaTemplateNotFoundException(topic));
    }
}
