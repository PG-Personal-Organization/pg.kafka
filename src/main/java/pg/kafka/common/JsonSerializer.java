package pg.kafka.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

@Log4j2
public class JsonSerializer<T> implements Serializer<T> {
    @Override
    public byte[] serialize(final @NonNull String topic, final @NonNull T data) {
        ObjectMapper objectMapper = Commons.defaultObjectMapper();
        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(data);
        } catch (final JsonProcessingException e) {
            log.error("Serialize error", e);
            throw new RuntimeException("Serialize error");
        }

        return bytes;
    }

    @Override
    public byte[] serialize(final @NonNull String topic, final @NonNull Headers headers, final @NonNull T data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
