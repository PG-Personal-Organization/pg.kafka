package pg.kafka.test;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
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
import pg.kafka.consumer.MessageHandler;
import pg.kafka.sender.EventSender;

import java.time.Duration;
import java.time.LocalDateTime;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;

@Log4j2
@KafkaIntegrationTest
@EmbeddedKafka(brokerProperties = {"transaction.max.timeout.ms=3600000"})
class TestKafkaImplementation {
    @Autowired
    private Environment environment;
    @Autowired
    private EventSender eventSender;
    @Autowired
    private MessageHandler<ProcessPaymentMessage> paymentMessageHandler;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final ArgumentCaptor<ProcessPaymentMessage> messageCaptor = ArgumentCaptor.forClass(ProcessPaymentMessage.class);

    @BeforeEach
    void verifyBroker() {
        Assertions.assertNotNull(embeddedKafkaBroker);
        log.info("Embedded Kafka Broker running at: {}", embeddedKafkaBroker.getBrokersAsString());
    }

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
        var message = new ProcessPaymentMessage("P_1", "O1", "DELIVERED", LocalDateTime.now());

        // when
        Assertions.assertDoesNotThrow(() -> eventSender.sendEvent(message));
        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> Mockito.verify(paymentMessageHandler).handleMessage(messageCaptor.capture()));

        // then
        Mockito.verify(paymentMessageHandler).handleMessage(any());
        Assertions.assertEquals(message, messageCaptor.getValue());
    }
}
