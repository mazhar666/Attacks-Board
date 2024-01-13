package Utilz.Deserializers;
import Events.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Arrays;

public class EventDeserializer implements DeserializationSchema<Event> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private Class<Event> tClass;

    public EventDeserializer() {
    }

    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        if (bytes == null)
            return null;
        Event data;
            data = objectMapper.readValue(bytes, Event.class);
        return data;
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }


    @Override
    public TypeInformation<Event> getProducedType() {
        return null;
    }
}
