package pg.kafka.consumer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.util.StopWatch;
import pg.kafka.message.Message;
import pg.kafka.topic.TopicName;
import pg.lib.common.spring.auth.HeaderNames;
import pg.lib.common.spring.storage.HeadersHolder;

import java.util.Map;
import java.util.UUID;

@Log4j2
@RequiredArgsConstructor
public class KafkaConsumer extends AbstractConsumerSeekAware implements MessageListener<String, Object> {
    private final MessageHandlerLocator messageHandlerLocator;
    private final HeadersHolder headersHolder;
    private final String consumerGroup;

    @Override
    @SuppressWarnings("unchecked")
    public void onMessage(final @NonNull ConsumerRecord<String, Object> message) {
        log.info("Consumer :'{}' process event from partition: {}-{}, and offset: {}, headers: {}",
                consumerGroup, message.topic(), message.partition(), message.offset(), storeAndGetHeaders(message.headers()));

        var handler = messageHandlerLocator.getMessageHandler(TopicName.of(message.topic()));

        if (handler == null) {
            log.warn("No handler found for topic: {} and message: {}, discarding message processing.", message.topic(), message.value());
            return;
        }

        StopWatch watch = new StopWatch();
        watch.start();
        try {
            log.debug("Processing with handler: {}-{} message: {}",
                    handler.getConsumerGroup(), handler.getClass().getSimpleName(), message.value());
            handler.handleMessage((Message) message.value());
        } catch (final Exception e) {
            log.error("Error while processing message", e);
        } finally {
            watch.stop();
            log.info("Consumer :'{}' processed event from partition: {}-{}, processing time: {} ms", consumerGroup,
                    message.topic(), message.partition(), watch.getTotalTimeMillis());
        }
    }

    private Map<String, String> storeAndGetHeaders(final Headers headers) {
        headers.forEach(it -> headersHolder.putHeader(it.key(), new String(it.value())));
        headersHolder.tryToGetHeader(HeaderNames.TRACE_ID).ifPresentOrElse(
                header -> {
                },
                () -> {
                    log.debug("No trace id header found, adding new one");
                    headersHolder.putHeader(HeaderNames.TRACE_ID, UUID.randomUUID().toString());
                });
        return headersHolder.getAllHeaders();
    }
}
