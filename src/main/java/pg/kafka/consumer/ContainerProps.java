package pg.kafka.consumer;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ContainerProps {
    public static final String EOS_MODE = "eosMode";
    public static final String ACK_MODE = "ackMode";
    public static final String ENABLE_TRANSACTION = "enableTransactional";
    public static final String TRANSACTION_TIMEOUT = "transactionTimeout";
    public static final String CONCURRENCY = "concurrency";
    public static final String RETRY = "retry";
}
