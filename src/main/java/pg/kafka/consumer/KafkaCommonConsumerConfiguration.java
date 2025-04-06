package pg.kafka.consumer;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.backoff.FixedBackOff;
import pg.kafka.common.Commons;
import pg.kafka.common.JsonDeserializer;
import pg.kafka.config.KafkaProperties;
import pg.kafka.config.KafkaPropertiesProvider;
import pg.kafka.config.MessagesDestinationConfig;
import pg.kafka.message.Message;
import pg.kafka.message.MessageDestination;
import pg.kafka.producer.DestinationResolver;
import pg.kafka.topic.TopicName;
import pg.lib.common.spring.storage.HeadersHolder;

import java.util.*;

@Log4j2
@Configuration
public class KafkaCommonConsumerConfiguration {

    private final ConfigurableBeanFactory beanFactory;
    private final DestinationResolver destinationResolver;

    @Lazy
    @Autowired
    @SuppressWarnings("checkstyle:HiddenField")
    public KafkaCommonConsumerConfiguration(final ConfigurableBeanFactory beanFactory,
                                            final DestinationResolver destinationResolver) {
        this.beanFactory = beanFactory;
        this.destinationResolver = destinationResolver;
    }

    @Value("${pg.kafka-transaction-manager.default-timeout:10}")
    private int transactionManagerDefaultTimeout;

    @Bean
    public MessagesDestinationConfig messagesDestinationConfig(final List<MessageDestination> messageDestinations) {
        return new MessagesDestinationConfig(messageDestinations);
    }

    @Bean
    public MessageHandlerLocator messageHandlerLocator(final @NonNull List<MessageHandler<?>> messageHandlers,
                                                       final @NonNull MessagesDestinationConfig messagesDestinationConfig) {
        return new MessageHandlerLocator(messageHandlers, messagesDestinationConfig);
    }

    @Bean
    public List<ConcurrentMessageListenerContainer<String, ? extends Message>> kafkaMessageListeners(
            final @NonNull MessagesDestinationConfig messagesDestinationConfig,
            final @NonNull KafkaPropertiesProvider kafkaPropertiesProvider,
            final @NonNull MessageHandlerLocator messageHandlerLocator,
            final @NonNull HeadersHolder headersHolder
    ) {
        List<ConcurrentMessageListenerContainer<String, ? extends Message>> listeners = new ArrayList<>();

        var destinations = messagesDestinationConfig.getDestinations();
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();
        var consumerConfigs = kafkaProperties.getConsumerConfigs();

        for (MessageDestination destination : destinations) {
            var handler = messageHandlerLocator.getMessageHandler(destination.getTopic());
            if (handler == null) {
                log.warn("No message handler found for topic: {}, skipping registration of message listener", destination.getTopic());
                continue;
            }

            var consumerConfig = Commons.defaultConsumerProperties();
            consumerConfig.putAll(consumerConfigs.getOrDefault(destination.getTopic(), new HashMap<>()));
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServer());
            String consumerGroup = (String) handler.getConsumerGroup().orElseGet(() -> destination.getTopic().getName());

            var consumerBeanName = handler.getConsumerGroup().isPresent()
                    ? destination.getTopic().getName() + "-listener-" + consumerGroup
                    : destination.getTopic().getName() + "-listener";
            var kafkaConsumer = new KafkaConsumer(messageHandlerLocator, headersHolder, consumerGroup);
            beanFactory.registerSingleton(consumerBeanName, kafkaConsumer);
            log.info("Registered kafka listener bean: {} from destination: {}", consumerBeanName, destination);

            var listenerContainerBeanName = consumerBeanName + "-container";
            ConcurrentMessageListenerContainer<String, ? extends Message> listenerContainer = listenerContainerBuilder()
                    .kafkaConsumer(kafkaConsumer)
                    .consumerGroup(consumerGroup)
                    .consumerConfig(consumerConfig)
                    .destination(destination)
                    .build();
            log.info("Registered kafka listener container bean: {} from config: {}, with consumer group: {}, listening to broker: {}",
                    listenerContainerBeanName, destination, consumerGroup, kafkaProperties.getBootstrapServer());
            listeners.add(listenerContainer);
        }

        log.info("Finished initializing kafka message listeners, listeners: {}", listeners);
        return listeners;
    }

    @SuppressWarnings("unchecked")
    @Builder(builderMethodName = "listenerContainerBuilder")
    public <T extends Message> ConcurrentMessageListenerContainer<String, T> kafkaListener(
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
            containerProperties.setKafkaAwareTransactionManager(kafkaTransactionManager(destination.getTopic()));
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

        var containerFactory = new ConcurrentMessageListenerContainer<String, T>(
                (ConsumerFactory<String, ? super T>) consumerFactory(consumerConfig, destination.getMessageClass()), containerProperties);
        containerFactory.setConcurrency(Integer.parseInt(consumerConfig.getOrDefault(ContainerProps.CONCURRENCY, 1).toString()));
        containerFactory.setBeanName(consumerGroup);

        var rollbackProcessor = new DefaultAfterRollbackProcessor<String, T>(null,
                new FixedBackOff(0L, Long.parseLong(consumerConfig.getOrDefault(ContainerProps.RETRY, 1L).toString())), null, false);
        containerFactory.setAfterRollbackProcessor(rollbackProcessor);

        return containerFactory;
    }

    public KafkaTransactionManager<String, Message> kafkaTransactionManager(final TopicName topicName) {
        var transactionManager = new KafkaTransactionManager<>(destinationResolver.getTemplate(topicName).getProducerFactory());
        transactionManager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
        transactionManager.setDefaultTimeout(transactionManagerDefaultTimeout);
        return transactionManager;
    }

    public <T extends Message> ConsumerFactory<String, T> consumerFactory(final Map<String, Object> consumerConfig,
                                                                          final Class<T> messageClass) {
        DefaultKafkaConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig);
        consumerFactory.setKeyDeserializer(new StringDeserializer());
        ErrorHandlingDeserializer<T> errorHandlingDeserializer = new ErrorHandlingDeserializer<>(new JsonDeserializer<>(messageClass));
        consumerFactory.setValueDeserializer(errorHandlingDeserializer);
        return consumerFactory;
    }
}
