package pg.kafka.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class KafkaPropertiesProvider {
    private final KafkaProperties kafkaProperties;
}
