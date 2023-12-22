package kafkaProducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class App {
    public static void main(String[] args) {
        runProducer();
    }
    static void runProducer(){
        Producer<String,String> producer = ProducerCreator.createProducer();
        for (int i = 0; i < IKafkaConstants.MESSAGE_COUNT; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-Kafka",""+i,"test");
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + i
                        + " with offset " + metadata.offset());
                Thread.sleep(100);
            }catch (Exception e ){
                System.out.println("error"+e);
            }
        }
    }
}
