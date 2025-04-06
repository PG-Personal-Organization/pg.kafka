package pg.kafka.message;

import lombok.Builder;
import lombok.Data;
import pg.kafka.topic.TopicName;

@Builder
@Data
public class MessageDestination {
    private final TopicName topic;
    private final Class<? extends Message> messageClass;
}
