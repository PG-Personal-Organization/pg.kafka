package pg.kafka.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@EqualsAndHashCode
@ToString
public class Message implements Serializable {
    protected final String id;
    protected final LocalDateTime timestamp;

    public Message() {
        this.id = "EVT-" + UUID.randomUUID();
        this.timestamp = LocalDateTime.now();
    }

    public Message(final LocalDateTime timeOfOccurrence) {
        this.id = "EVT-" + UUID.randomUUID();
        this.timestamp = timeOfOccurrence;
    }

    @JsonCreator
    @SuppressWarnings({"checkstyle:FinalParameters", "checkstyle:HiddenField"})
    public Message(final @JsonProperty("id") String id,
                   final @JsonProperty("timestamp") LocalDateTime timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }
}
