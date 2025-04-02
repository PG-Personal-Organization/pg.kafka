package pg.kafka.producer.exceptions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import pg.kafka.topic.TopicName;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaTemplateNotFoundException extends RuntimeException {
    public KafkaTemplateNotFoundException(final @NonNull TopicName topic) {
        super("""
                Could not find destination template for topic: %s
                """.formatted(topic.getName())
        );
    }
}
