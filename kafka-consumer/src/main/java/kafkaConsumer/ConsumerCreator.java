package kafkaConsumer;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class ConsumerCreator {
    public static FlinkKafkaConsumer<String> createConsumer(){

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", IKafkaConstants.KAFKA_BROKERS);
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        String groupName = "test";
        properties.setProperty("group.id", groupName);
        FlinkKafkaConsumer<String> myConsumer =new FlinkKafkaConsumer<>(IKafkaConstants.TOPIC_NAME,new SimpleStringSchema(),properties);
        myConsumer.setStartFromLatest();
        return myConsumer;
    }
}
