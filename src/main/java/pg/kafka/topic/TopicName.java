package pg.kafka.topic;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor(staticName = "of")
public class TopicName {
    @NonNull
    @NotBlank
    private String name;
}
