package pg.kafka.producer.exceptions;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import pg.kafka.message.Message;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DestinationNotFoundException extends RuntimeException {
    public DestinationNotFoundException(final Class<? extends Message> messageClass) {
        super("""
                Could not find destination topic for message class: %s
                """.formatted(messageClass.getName())
        );
    }
}
