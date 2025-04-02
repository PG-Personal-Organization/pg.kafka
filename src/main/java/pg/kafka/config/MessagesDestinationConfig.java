package pg.kafka.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import pg.kafka.message.MessageDestination;

import java.util.ArrayList;
import java.util.List;

@Getter
@Configuration
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class MessagesDestinationConfig {
    private final List<MessageDestination> destinations = new ArrayList<>();
}
