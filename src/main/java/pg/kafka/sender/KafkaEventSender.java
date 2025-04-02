package pg.kafka.sender;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import pg.kafka.message.Message;
import pg.kafka.message.MessageHeaders;
import pg.kafka.producer.DestinationResolver;
import pg.kafka.topic.TopicName;
import pg.lib.common.spring.auth.HeaderNames;
import pg.lib.common.spring.storage.HeadersHolder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

@Log4j2
@RequiredArgsConstructor
public class KafkaEventSender implements EventSender {
    private final DestinationResolver destinationResolver;
    private final HeadersHolder headersHolder;

    @Override
    public <T extends Message> void sendEvent(final @NonNull T message) {
        try {
            TopicName topic = destinationResolver.resolveDestination(message.getClass());
            KafkaTemplate<String, T> kafkaTemplate = destinationResolver.getTemplate(topic);

            var headers = new HashMap<>(headersHolder.getAllHeaders());
            headers.putIfAbsent(HeaderNames.TRACE_ID, UUID.randomUUID().toString());
            headers.put(MessageHeaders.TYPE_ID, message.getClass().getCanonicalName());
            headers.put(MessageHeaders.EVT_ID, message.getId());

            log.debug("Sending to topic: {} following message: {} with headers: {} and transaction: {}",
                    topic, message, headers, TransactionSynchronizationManager.isActualTransactionActive());

            SendResult<String, T> sendResult;
            ProducerRecord<String, T> producerRecord = new ProducerRecord<>(topic.getName(), message);

            headers.forEach((key, value) -> producerRecord.headers().add(key, value.getBytes(StandardCharsets.UTF_8)));

            sendResult = kafkaTemplate.send(producerRecord).get();
            log.debug("Sending result  {}", sendResult.getRecordMetadata());
        } catch (final Exception e) {
            log.error("Error while sending/commiting message", e);
        }
    }
}
