package pg.kafka.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

@Log4j2
public class JsonDeserializer<T> implements Deserializer<T> {
    private final Type type;

    public JsonDeserializer() {
        Type superClass = this.getClass().getGenericSuperclass();
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    @Override
    public T deserialize(final @NonNull String topic, final byte[] data) {
        ObjectMapper objectMapper =  Commons.defaultObjectMapper();
        try {
            JavaType javaType = objectMapper.getTypeFactory().constructType(type);
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), javaType);
        } catch (IOException e) {
            log.error("Deserialize error", e);
            throw new RuntimeException("Serialize error");
        }
    }
}