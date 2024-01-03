package Consumer;
import Utilz.Constants.IConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class Consumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source=createSource();
        DataStream<String> kafkaStream=env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStream.print();
        env.execute("test");
    }

private static KafkaSource<String> createSource(){

    return KafkaSource.<String>builder()
            .setBootstrapServers(IConstants.KAFKA_BROKERS)
            .setTopics(IConstants.TOPIC_NAME)
            .setGroupId("test-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
}

}
