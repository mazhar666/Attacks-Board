package Producer;

import Utilz.CSVGenerator;
import Utilz.Constants.IConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Producer {
    public static void main(String[] args) {
    Properties props= getProperties();
    int keyIndex=0;
    try(org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props)){
        do {
            String key = keyIndex + "";
            String data = CSVGenerator.getNextLine();
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(IConstants.TOPIC_NAME, key, data);
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("Produced with key: " + key + " Data: " + data + " Offset: " + recordMetadata.offset());
            Thread.sleep(1000);
            keyIndex++;
        } while (CSVGenerator.getNextLine()!= null);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            System.out.println("ExecutionException");
            throw new RuntimeException(e);
        }

    }



    public static Properties getProperties(){
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IConstants.KAFKA_BROKERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, IConstants.CLIENT_ID);
        return properties;
    }

}
