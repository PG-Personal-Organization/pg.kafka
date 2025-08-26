package pg.kafka.topic;

import jakarta.validation.constraints.Positive;
import lombok.*;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class TopicDefinition {
    public static final TopicDefinition.TopicDefinitionBuilder DEFAULT = TopicDefinition.builder()
            .partitions(TopicDefaults.partitionsCount)
            .replicationFactor(TopicDefaults.replicationFactor);

    @NonNull
    private final TopicName topic;
    @Positive
    private final int partitions;
    @Positive
    private final int replicationFactor;
}
