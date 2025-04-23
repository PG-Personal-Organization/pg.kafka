package pg.kafka.consumer;

import lombok.*;

import java.util.HashMap;
import java.util.Map;

@ToString
@Getter
@AllArgsConstructor
@Builder
public class ConsumerConfig {
    @Singular
    private final Map</* org.apache.kafka.clients.consumer.ConsumerConfig as keys */String, Object> properties = new HashMap<>();

    @NonNull
    private final String consumerName;
}
