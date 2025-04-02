package pg.kafka.message;

import lombok.Data;
import pg.kafka.topic.TopicName;

@Data
public class MessageDestination {
    private final TopicName topic;
    private final Class<? extends Message> messageClass;
}
