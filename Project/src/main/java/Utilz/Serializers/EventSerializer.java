package Utilz.Serializers;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class EventSerializer<Event> implements Serializer<Event> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public EventSerializer(){}


    @Override
    public byte[] serialize(String s, Event event) {
        if (event == null){
            System.out.println("Null received at serializing");
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
