package pg.kafka.config;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import pg.kafka.businesscase.ProcessPaymentMessage;
import pg.kafka.consumer.MessageHandler;
import pg.kafka.message.MessageDestination;
import pg.kafka.topic.TopicDefinition;
import pg.kafka.topic.TopicName;
import pg.lib.common.spring.auth.NoOpHeaderAuthenticationConfiguration;
import pg.lib.common.spring.config.CommonModuleConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Import({
        KafkaConfiguration.class,
        CommonModuleConfiguration.class,
        NoOpHeaderAuthenticationConfiguration.class
})
@Configuration
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaTestConfiguration {

    @Bean
    public TopicDefinition paymentProcessingBatchTopicDefinition() {
        return TopicDefinition.DEFAULT
                .topic(TopicName.of("payment-processing-batch-topic"))
                .build();
    }

    @Bean
    public MessageDestination processPaymentMessageDestination() {
        return MessageDestination.builder()
                .topic(TopicName.of("payment-processing-batch-topic"))
                .messageClass(ProcessPaymentMessage.class)
                .build();
    }

    @Bean
    public MessageHandler<ProcessPaymentMessage> processPaymentMessageMessageHandler() {
        return Mockito.spy(new ProcessPaymentMessageHandler());
    }

    @Log4j2
    public static class ProcessPaymentMessageHandler implements MessageHandler<ProcessPaymentMessage> {
        private final List<ProcessPaymentMessage> messages = new ArrayList<>();

        @Override
        public void handleMessage(@NonNull ProcessPaymentMessage message) {
            messages.add(message);
            log.info("Message received: {}", message);
            log.info("Messages received overall: {}", messages);
        }

        @Override
        public Class<ProcessPaymentMessage> getMessageType() {
            return ProcessPaymentMessage.class;
        }

        @Override
        public Optional<String> getConsumerGroup() {
            return Optional.of("ProcessPaymentMessage-ConsumerGroup");
        }

        @Override
        public String toString() {
            if (getConsumerGroup().isEmpty()) {
                return this.getClass().getCanonicalName();
            }
            return this.getClass().getCanonicalName() + "@" + getConsumerGroup();
        }
    }
}
