package pg.kafka.consumer;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.springframework.lang.Nullable;
import pg.kafka.config.MessagesDestinationConfig;
import pg.kafka.message.Message;
import pg.kafka.message.MessageDestination;
import pg.kafka.topic.TopicName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Log4j2
public class MessageHandlerLocator {
    private final Map<Class<? extends Message>, MessageHandler<? extends Message>> messageHandlerMap;
    private final Map<TopicName, Class<? extends Message>> topicMessageMap;

    public MessageHandlerLocator(final @NonNull List<MessageHandler<?>> messageHandlers,
                                 final @NonNull MessagesDestinationConfig messagesDestinationConfig) {
        this.messageHandlerMap = messageHandlers.stream()
                .collect(HashMap::new, (m, v) -> m.put(v.getMessageType(), v), HashMap::putAll);
        log.debug("Initial message handler map: {}", messageHandlerMap);
        Map<TopicName, Class<? extends Message>> topics = new java.util.HashMap<>();

        var destinations = messagesDestinationConfig.getDestinations();
        for (MessageDestination destination : destinations) {
            var handler = messageHandlerMap.get(destination.getMessageClass());
            if (handler == null) {
                log.warn("No message handler found for message class, omitting handler registration for message class: {}, and topic: {}",
                        destination.getMessageClass(), destination.getTopic());
                continue;
            }
            topics.put(destination.getTopic(), destination.getMessageClass());
        }
        this.topicMessageMap = topics;
        this.topicMessageMap.forEach((topicName, messageClass) -> {
            if (!this.messageHandlerMap.containsKey(messageClass)) {
                this.messageHandlerMap.remove(messageClass);
            }
        });
        log.debug("Finished initializing message handler locator, message handler map: {}, topics: {}", messageHandlerMap, topicMessageMap);
    }

    @SuppressWarnings("raw")
    public @Nullable MessageHandler getMessageHandler(final @NonNull TopicName topic) {
        return Optional.ofNullable(topicMessageMap.get(topic))
                .map(messageHandlerMap::get)
                .orElse(null);
    }

    @SuppressWarnings("raw")
    public MessageHandler getMessageHandler(final @NonNull Class<? extends Message> messageClass) {
        return messageHandlerMap.get(messageClass);
    }
}
