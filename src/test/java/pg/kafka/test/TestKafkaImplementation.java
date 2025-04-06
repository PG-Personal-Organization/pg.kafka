package pg.kafka.test;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import pg.kafka.businesscase.ProcessPaymentMessage;
import pg.kafka.config.KafkaIntegrationTest;
import pg.kafka.config.KafkaPropertiesProvider;
import pg.kafka.consumer.MessageHandler;
import pg.kafka.sender.EventSender;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;

@Slf4j
@KafkaIntegrationTest
//@EmbeddedKafka
class TestKafkaImplementation {
    @Autowired
    private Environment environment;
    @Autowired
    private EventSender eventSender;
    @Autowired
    private MessageHandler<ProcessPaymentMessage> paymentMessageHandler;
//    @Autowired
//    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ArgumentCaptor<ProcessPaymentMessage> messageCaptor = ArgumentCaptor.forClass(ProcessPaymentMessage.class);

//    @BeforeEach
//    void verifyBroker() {
//        Assertions.assertNotNull(embeddedKafkaBroker);
//        log.info("Embedded Kafka Broker running at: {}", embeddedKafkaBroker.getBrokersAsString());
//    }

    @Test
    void shouldStartWithKafkaConfiguration() {
        Assertions.assertNotNull(environment);
        Assertions.assertNotNull(eventSender);
        Assertions.assertNotNull(paymentMessageHandler);
    }

    @Test
    @SneakyThrows
    void shouldSendAndProcessPaymentMessages() {
        // given
//        Mockito.doNothing().when(paymentMessageHandler).handleMessage(messageCaptor.capture());
        var message = new ProcessPaymentMessage("P_1", "O1", "DELIVERED", LocalDateTime.now());

        // when
        Assertions.assertDoesNotThrow(() -> eventSender.sendEvent(message));
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> Mockito.verify(paymentMessageHandler).handleMessage(any()));

        // then
        Mockito.verify(paymentMessageHandler).handleMessage(any());
        Assertions.assertEquals(message, messageCaptor.getValue());
    }
}
