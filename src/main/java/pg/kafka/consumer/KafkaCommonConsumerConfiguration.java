package pg.kafka.consumer;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.backoff.FixedBackOff;
import pg.kafka.common.Commons;
import pg.kafka.common.KafkaJsonDeserializer;
import pg.kafka.config.KafkaConfigurationProvider;
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

    @Value("${pg.kafka-transaction-manager.default-timeout:10}")
    private int transactionManagerDefaultTimeout;

    @Bean
    @Lazy(false)
    public MessagesDestinationConfig messagesDestinationConfig(
            final ObjectProvider<List<MessageDestination>> messageDestinationsProvider) {
        var destinations = messageDestinationsProvider.getIfAvailable(Collections::emptyList);
        log.debug("Initializing MessagesDestinationConfig with {} destinations", destinations.size());
        return new MessagesDestinationConfig(destinations);
    }

    @Bean
    @Lazy(false)
    public MessageHandlerLocator messageHandlerLocator(
            final ObjectProvider<List<MessageHandler<?>>> messageHandlersProvider,
            final MessagesDestinationConfig messagesDestinationConfig) {
        var handlers = messageHandlersProvider.getIfAvailable(Collections::emptyList);
        log.debug("Initializing MessageHandlerLocator with {} handlers", handlers.size());
        return new MessageHandlerLocator(handlers, messagesDestinationConfig);
    }

    @Bean
    @Lazy(false)
    public List<ConcurrentMessageListenerContainer<String, ? extends Message>> kafkaMessageListeners(
            final MessagesDestinationConfig messagesDestinationConfig,
            final KafkaPropertiesProvider kafkaPropertiesProvider,
            final KafkaConfigurationProvider kafkaConfigurationProvider,
            final MessageHandlerLocator messageHandlerLocator,
            final ObjectProvider<HeadersHolder> headersHolderProvider,
            final ObjectProvider<ConfigurableBeanFactory> beanFactoryProvider,
            final ObjectProvider<DestinationResolver> destinationResolverProvider
    ) {
        log.debug("Initializing kafka message listeners");

        var headersHolder = headersHolderProvider.getIfAvailable();
        var beanFactory = beanFactoryProvider.getIfAvailable();
        var destinationResolver = destinationResolverProvider.getIfAvailable();

        if (headersHolder == null || beanFactory == null || destinationResolver == null) {
            log.warn("Skipping kafka listeners: missing required beans [HeadersHolder={}, BeanFactory={}, DestinationResolver={}]",
                    headersHolder, beanFactory, destinationResolver);
            return List.of();
        }

        List<ConcurrentMessageListenerContainer<String, ? extends Message>> listeners = new ArrayList<>();
        var destinations = messagesDestinationConfig.getDestinations();
        KafkaProperties kafkaProperties = kafkaPropertiesProvider.getKafkaProperties();
        var consumerConfigs = kafkaProperties.getConsumerConfigs();

        for (MessageDestination destination : destinations) {
            var handler = messageHandlerLocator.getMessageHandler(destination.getTopic());
            if (handler == null) {
                log.warn("No message handler found for topic: {}, skipping registration", destination.getTopic());
                continue;
            }

            String consumerGroup = (String) handler.getConsumerGroup().orElseGet(() -> destination.getTopic().getName());

            var consumerConfig = Commons.defaultConsumerProperties();
            var config = kafkaConfigurationProvider.getConsumerConfig(consumerGroup);
            config.ifPresent(cfg -> consumerConfig.putAll(cfg.getProperties()));

            consumerConfig.putAll(consumerConfigs.getOrDefault(destination.getTopic(), new HashMap<>()));
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServer());

            var consumerBeanName = handler.getConsumerGroup().isPresent()
                    ? destination.getTopic().getName() + "-listener-" + consumerGroup
                    : destination.getTopic().getName() + "-listener";

            var kafkaConsumer = new KafkaConsumer(messageHandlerLocator, headersHolder, consumerGroup);
            beanFactory.registerSingleton(consumerBeanName, kafkaConsumer);
            log.debug("Registered kafka listener bean: {} for destination: {}", consumerBeanName, destination);

            var listenerContainerBeanName = consumerBeanName + "-container";
            ConcurrentMessageListenerContainer<String, ? extends Message> listenerContainer = listenerContainerBuilder()
                    .kafkaConsumer(kafkaConsumer)
                    .consumerGroup(consumerGroup)
                    .consumerConfig(consumerConfig)
                    .destination(destination)
                    .destinationResolver(destinationResolver)
                    .build();
            log.debug("Registered kafka listener container: {} (group: {}, broker: {})",
                    listenerContainerBeanName, consumerGroup, kafkaProperties.getBootstrapServer());

            listeners.add(listenerContainer);
        }

        if (listeners.isEmpty()) {
            log.warn("No kafka listeners registered (no matching destinations/handlers)");
        } else {
            listeners.forEach(ConcurrentMessageListenerContainer::start);
            log.info("Started {} kafka message listeners", listeners.size());
        }
        return listeners;
    }

    @SuppressWarnings({"unchecked", "unused"})
    @Builder(builderMethodName = "listenerContainerBuilder")
    public <T extends Message> ConcurrentMessageListenerContainer<String, T> kafkaListener(
            final @NonNull KafkaConsumer kafkaConsumer,
            final @NonNull String consumerGroup,
            final @NonNull Map<String, Object> consumerConfig,
            final @NonNull MessageDestination destination,
            final @NonNull DestinationResolver destinationResolver) {
        ContainerProperties containerProperties = new ContainerProperties(destination.getTopic().getName());
        containerProperties.setGroupId(consumerGroup);
        containerProperties.setMessageListener(kafkaConsumer);
        containerProperties.setAckMode(ContainerProperties.AckMode.valueOf((String) consumerConfig.get(ContainerProps.ACK_MODE)));
        containerProperties.setEosMode(ContainerProperties.EOSMode.valueOf((String) consumerConfig.get(ContainerProps.EOS_MODE)));

        var isTransactional = (boolean) consumerConfig.get(ContainerProps.ENABLE_TRANSACTION);
        if (isTransactional) {
            DefaultTransactionDefinition txDef = new DefaultTransactionDefinition();
            txDef.setTimeout(consumerConfig.get(ContainerProps.TRANSACTION_TIMEOUT) == null
                    ? 0 : (int) consumerConfig.get(ContainerProps.TRANSACTION_TIMEOUT));
            containerProperties.setKafkaAwareTransactionManager(kafkaTransactionManager(destination.getTopic(), destinationResolver));
            containerProperties.setTransactionDefinition(txDef);
        }

        Properties props = new Properties();
        var consumerProps = consumerConfig.entrySet().stream()
                .filter(e -> !Set.of(ContainerProps.ACK_MODE, ContainerProps.EOS_MODE,
                        ContainerProps.ENABLE_TRANSACTION, ContainerProps.TRANSACTION_TIMEOUT).contains(e.getKey()))
                .collect(HashMap::new, (m, v) -> m.put(v.getKey(), v.getValue()), HashMap::putAll);
        props.putAll(consumerProps);
        containerProperties.setKafkaConsumerProperties(props);

        var container = new ConcurrentMessageListenerContainer<>(
                (ConsumerFactory<String, T>) consumerFactory(consumerConfig, destination.getMessageClass()),
                containerProperties);

        container.setConcurrency(Integer.parseInt(
                consumerConfig.getOrDefault(ContainerProps.CONCURRENCY, 1).toString()));
        container.setBeanName(consumerGroup);

        var errorHandler = new DefaultErrorHandler(
                new FixedBackOff(0L, Long.parseLong(
                        consumerConfig.getOrDefault(ContainerProps.RETRY, 1L).toString()))
        );
        container.setCommonErrorHandler(errorHandler);

        return container;
    }

    public KafkaTransactionManager<String, Message> kafkaTransactionManager(
            final TopicName topicName, final DestinationResolver destinationResolver) {
        var txManager = new KafkaTransactionManager<>(
                destinationResolver.getTemplate(topicName).getProducerFactory());
        txManager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_NEVER);
        txManager.setDefaultTimeout(transactionManagerDefaultTimeout);
        return txManager;
    }

    public <T extends Message> ConsumerFactory<String, T> consumerFactory(
            final Map<String, Object> consumerConfig,
            final Class<T> messageClass) {
        var consumerFactory = new DefaultKafkaConsumerFactory<String, T>(consumerConfig);
        consumerFactory.setKeyDeserializer(new StringDeserializer());
        var errorHandlingDeserializer = new ErrorHandlingDeserializer<>(new KafkaJsonDeserializer<>(messageClass));
        consumerFactory.setValueDeserializer(errorHandlingDeserializer);
        return consumerFactory;
    }
}
