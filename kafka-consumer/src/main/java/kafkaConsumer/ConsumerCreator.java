package kafkaConsumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerCreator {
    public static Consumer<String,String> createConsumer(){

        Properties properties = new Properties();
        properties.put("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
        properties.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        String groupName = "consumer-group1";
        properties.put("group.id", groupName);

        return new KafkaConsumer<>(properties);
    }
}
