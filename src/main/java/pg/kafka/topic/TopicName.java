package pg.kafka.topic;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(staticName = "of")
public class TopicName {
    private String name;
}
