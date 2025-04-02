package pg.kafka.consumer;

import lombok.NonNull;
import pg.kafka.message.Message;

import java.util.Optional;

public interface MessageHandler<T extends Message> {
    void handleMessage(@NonNull T message);

    Class<T> getMessageType();

    default Optional<String> getConsumerGroup() {
        return Optional.empty();
    }
}
