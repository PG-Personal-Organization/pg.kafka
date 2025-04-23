package pg.kafka.producer;

import lombok.*;
import pg.kafka.topic.TopicName;

import java.util.HashMap;
import java.util.Map;

@ToString
@Getter
@AllArgsConstructor
@Builder
public class ProducerConfig {
    @Singular
    private final Map</* org.apache.kafka.clients.producer.ProducerConfig as keys */String, Object> properties = new HashMap<>();

    @NonNull
    private final TopicName topicName;
}
