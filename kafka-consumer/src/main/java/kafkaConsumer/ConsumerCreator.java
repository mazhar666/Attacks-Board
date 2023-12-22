package kafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCreator {
    public static KafkaSource<String> createConsumer(){

        return KafkaSource.<String>builder()
                .setBootstrapServers(IKafkaConstants.KAFKA_BROKERS)
                .setTopics(IKafkaConstants.TOPIC_NAME)
                .setGroupId("consumer-group1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    }
}
