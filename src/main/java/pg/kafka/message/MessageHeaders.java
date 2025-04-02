package pg.kafka.message;

import lombok.experimental.UtilityClass;

@UtilityClass
public class MessageHeaders {
    public static final String MESSAGE_TYPE = "__TypeId__";
    public static final String TYPE_ID = "app-message-type";
    public static final String EVT_ID = "app-message-id";
}
