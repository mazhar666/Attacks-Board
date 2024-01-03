import kafkaConsumer.ConsumerCreator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class App {
    public static void main(String[] args) throws Exception {
        runConsumer();
    }
    static void runConsumer() throws Exception {
        FlinkKafkaConsumer<String> consumer = ConsumerCreator.createConsumer();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> stream = env.addSource(consumer);
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
        env.execute("test" );
    }

}
