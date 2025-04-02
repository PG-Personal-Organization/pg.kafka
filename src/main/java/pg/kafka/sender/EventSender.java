package pg.kafka.sender;

import lombok.NonNull;
import pg.kafka.message.Message;

public interface EventSender {
    <T extends Message> void sendEvent(@NonNull T message);
}
