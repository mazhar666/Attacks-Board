import kafkaConsumer.ConsumerCreator;
import kafkaConsumer.IKafkaConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;

public class App {
    public static void main(String[] args) throws Exception {
        runConsumer();
    }
    static void runConsumer() throws Exception {
        KafkaSource<String> consumer = ConsumerCreator.createConsumer();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> stream = env.fromSource(consumer, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.print();
        env.execute("test job" );
    }

}
