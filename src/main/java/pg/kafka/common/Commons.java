package pg.kafka.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.kafka.listener.ContainerProperties;
import pg.kafka.consumer.ContainerProps;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@UtilityClass
public class Commons {

    /**
     * Default consumer properties {@link org.apache.kafka.clients.consumer.ConsumerConfig}
     * */
    private final int defaultMaxPollRecords = 1;
    private final int defaultConcurrency = 1;
    private final int defaultRetry = 0;
    private final int defaultSessionTimeoutMillis = 30000;
    private final int defaultMaxPollIntervalMillis = 1860000;
    private final int defaultHeartbeatIntervalMillis = 9000;
    private final int defaultConsumerTransactionTimeout = 1800;
    private final boolean defaultEnableTransactional = true;

    /**
    * Default producer properties {@link org.apache.kafka.clients.producer.ProducerConfig}
    * */
    private final boolean defaultEnableIdempotence = true;
    private final int defaultRequestTimeoutMillis = 120000;
    private final int defaultLingerMillis = 0;
    private final int defaultRetries = 0x7fffffff;
    private final int defaultMaxRequestSize = 5242880;
    private final int defaultMaxBlockMillis = 30000;
    private final int defaultProducerTransactionTimeout = 3600000;

    public static ObjectMapper defaultObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return objectMapper;
    }

    public Map<String, Object> defaultProducerProperties() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, defaultEnableIdempotence);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, defaultRequestTimeoutMillis);
        props.put(ProducerConfig.LINGER_MS_CONFIG, defaultLingerMillis);
        props.put(ProducerConfig.RETRIES_CONFIG, defaultRetries);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, defaultMaxRequestSize);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, defaultMaxBlockMillis);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, defaultProducerTransactionTimeout);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

    public Map<String, Object> defaultConsumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, defaultMaxPollRecords);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, defaultSessionTimeoutMillis);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, defaultMaxPollIntervalMillis);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, defaultHeartbeatIntervalMillis);

        props.put(ContainerProps.EOS_MODE, ContainerProperties.EOSMode.V2.name());
        props.put(ContainerProps.ACK_MODE, ContainerProperties.AckMode.RECORD.name());
        props.put(ContainerProps.ENABLE_TRANSACTION, defaultEnableTransactional);
        props.put(ContainerProps.TRANSACTION_TIMEOUT, defaultConsumerTransactionTimeout);
        props.put(ContainerProps.CONCURRENCY, defaultConcurrency);
        props.put(ContainerProps.RETRY, defaultRetry);

        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }
}
