package pg.kafka.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@Import({
        KafkaTestConfiguration.class
})
@SpringBootApplication
public class KafkaTestApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaTestApplication.class, args);
    }
}
