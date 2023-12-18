package kafkaConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;

public class App {
    public static void main(String[] args) {
        runConsumer();
    }
    static void runConsumer(){
        Consumer<String,String> consumer = ConsumerCreator.createConsumer();
        consumer.subscribe(Collections.singletonList(IKafkaConstants.TOPIC_NAME));
        int noMessagesFound=0;
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1000));
            if (records.count() ==0){
                noMessagesFound++;
                if (noMessagesFound> IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT){
                    break;
                }else {
                    continue;
                }
            }
            records.forEach(record->{
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        System.out.println("closing");
        consumer.close();
    }

}
