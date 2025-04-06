package pg.kafka.common;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Log4j2
public class KafkaJsonDeserializer<T> extends JsonDeserializer<T> {

    public KafkaJsonDeserializer(final Class<T> clazz) {
        super(Commons.defaultObjectMapper());
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
        typeMapper.setIdClassMapping(Map.of(clazz.getCanonicalName(), clazz));
        setTypeMapper(typeMapper);
    }
}