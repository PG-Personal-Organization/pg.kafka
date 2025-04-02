package pg.kafka.consumer;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.backoff.FixedBackOff;
import pg.kafka.common.Commons;
import pg.kafka.common.JsonDeserializer;
import pg.kafka.config.KafkaPropertiesProvider;
import pg.kafka.config.MessagesDestinationConfig;
import pg.kafka.message.MessageDestination;
import pg.lib.common.spring.storage.HeadersHolder;

import java.util.*;

@Log4j2
@Configuration
@RequiredArgsConstructor(onConstructor_ = @Autowired)
public class KafkaCommonConsumerConfiguration {

    private final ConfigurableBeanFactory beanFactory;

    @Bean
    public MessageHandlerLocator messageHandlerLocator(final @NonNull List<MessageHandler<?>> messageHandlers,
                                                       final @NonNull MessagesDestinationConfig messagesDestinationConfig) {
        return new MessageHandlerLocator(messageHandlers, messagesDestinationConfig);
    }

    @Bean
    public List<ConcurrentMessageListenerContainer<String, Object>> kafkaMessageListeners(
            final @NonNull MessagesDestinationConfig messagesDestinationConfig,
            final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider,
            final @NonNull MessageHandlerLocator messageHandlerLocator,
            final @NonNull HeadersHolder headersHolder
    ) {
        List<ConcurrentMessageListenerContainer<String, Object>> listeners = new ArrayList<>();

        var destinations = messagesDestinationConfig.getDestinations();
        var consumerConfigs = kafkaPropertiesProvider.getKafkaProperties().getConsumerConfigs();

        for (MessageDestination destination : destinations) {
            var handler = messageHandlerLocator.getMessageHandler(destination.getTopic());
            if (handler == null) {
                log.warn("No message handler found for topic: {}, skipping registration of message listener", destination.getTopic());
                continue;
            }

            var consumerConfig = Commons.defaultConsumerProperties();
            consumerConfig.putAll(consumerConfigs.getOrDefault(destination.getTopic(), new HashMap<>()));
            String consumerGroup = (String) handler.getConsumerGroup().orElseGet(() -> destination.getTopic().getName());

            var consumerBeanName = destination.getTopic().getName() + "-listener-" + consumerGroup;
            var kafkaConsumer = new KafkaConsumer(messageHandlerLocator, headersHolder, consumerGroup);
            beanFactory.registerSingleton(consumerBeanName, kafkaConsumer);
            log.info("Registered kafka listener bean: {} from destination: {}", consumerBeanName, destination);

            var listenerContainerBeanName = consumerBeanName + "-container";
            ConcurrentMessageListenerContainer<String, Object> listenerContainer = listenerContainerBuilder()
                    .kafkaConsumer(kafkaConsumer)
                    .consumerGroup(consumerGroup)
                    .consumerConfig(consumerConfig)
                    .destination(destination)
                    .build();
            log.info("Registered kafka listener container bean: {} from config: {}", listenerContainerBeanName, destination);
            listeners.add(listenerContainer);
        }

        return listeners;
    }

    @Builder(builderMethodName = "listenerContainerBuilder")
    public ConcurrentMessageListenerContainer<String, Object> kafkaListener(
            final @NonNull KafkaConsumer kafkaConsumer,
            final @NonNull String consumerGroup,
            final @NonNull Map<String, Object> consumerConfig,
            final @NonNull MessageDestination destination) {
        ContainerProperties containerProperties = new ContainerProperties(destination.getTopic().getName());
        containerProperties.setGroupId(consumerGroup);
        containerProperties.setMessageListener(kafkaConsumer);
        containerProperties.setAckMode(ContainerProperties.AckMode.valueOf((String) consumerConfig.get(ContainerProps.ACK_MODE)));
        containerProperties.setEosMode(ContainerProperties.EOSMode.valueOf((String) consumerConfig.get(ContainerProps.EOS_MODE)));

        var isTransactional = (boolean) consumerConfig.get(ContainerProps.ENABLE_TRANSACTION);
        if (isTransactional) {
            DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
            transactionDefinition.setTimeout(consumerConfig.get(ContainerProps.TRANSACTION_TIMEOUT) == null
                    ? 0 : (int) consumerConfig.get(ContainerProps.TRANSACTION_TIMEOUT));
            containerProperties.setKafkaAwareTransactionManager(beanFactory.getBean(KafkaTransactionManager.class));
            containerProperties.setTransactionDefinition(transactionDefinition);
        }

        Properties props = new Properties();
        var consumerProps = consumerConfig.entrySet().stream()
                .filter(e -> !e.getKey().equals(ContainerProps.ACK_MODE))
                .filter(e -> !e.getKey().equals(ContainerProps.EOS_MODE))
                .filter(e -> !e.getKey().equals(ContainerProps.ENABLE_TRANSACTION))
                .filter(e -> !e.getKey().equals(ContainerProps.TRANSACTION_TIMEOUT))
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
        props.putAll(consumerProps);
        containerProperties.setKafkaConsumerProperties(props);

        var containerFactory = new ConcurrentMessageListenerContainer<>(consumerFactory(consumerConfig), containerProperties);
        containerFactory.setConcurrency((Integer) consumerConfig.getOrDefault(ContainerProps.CONCURRENCY, 1));
        containerFactory.setBeanName(consumerGroup);

        var rollbackProcessor = new DefaultAfterRollbackProcessor<String, Object>(null,
                new FixedBackOff(0L, (Long) consumerConfig.getOrDefault(ContainerProps.RETRY, 1)), null, true);
        containerFactory.setAfterRollbackProcessor(rollbackProcessor);

        return containerFactory;
    }

    @SuppressWarnings("unchecked")
    public ConsumerFactory<String, Object> consumerFactory(final Map<String, Object> consumerConfig) {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig);
        consumerFactory.setKeyDeserializer((Deserializer<String>) consumerConfig.getOrDefault(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));

        Deserializer<Object> errorHandlingDeserializer = new ErrorHandlingDeserializer<>((Deserializer<Object>) consumerConfig.getOrDefault(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class));
        consumerFactory.setValueDeserializer(errorHandlingDeserializer);
        return consumerFactory;
    }
}
