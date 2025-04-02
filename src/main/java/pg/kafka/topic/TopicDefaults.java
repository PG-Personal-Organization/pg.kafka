package pg.kafka.topic;

import lombok.experimental.UtilityClass;

@UtilityClass
@SuppressWarnings("checkstyle:MagicNumber")
public class TopicDefaults {
    public final int partitionsCount = 10;
    public final int replicationFactor = 1;
}
