package pg.kafka.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import pg.kafka.message.MessageDestination;

import java.util.List;

@Getter
@AllArgsConstructor
public class MessagesDestinationConfig {
    private final List<MessageDestination> destinations;
}
