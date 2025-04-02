package pg.kafka.topic;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class TopicConfig {
    private TopicName topic;
    private int partitions = TopicDefaults.partitionsCount;
    private int replicationFactor = TopicDefaults.replicationFactor;

    /**
    * {@link org.apache.kafka.common.config.TopicConfig as key}
    * */
    private Map<String, Object> config = new HashMap<>();
}
