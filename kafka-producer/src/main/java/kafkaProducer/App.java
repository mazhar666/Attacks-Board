package kafkaProducer;

import dataGenerator.CSVGenerator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        runProducer();
    }

    static void runProducer() throws IOException {
        Producer<String,JSONObject> producer = ProducerCreator.createProducer();
        BufferedReader bufferedReader = CSVGenerator.getScanner();
        String line ;
        String splitBy = ",";
        int lineIndex=1;
        while ((line = bufferedReader.readLine()) != null){
            try {
                JSONObject attack= createAttackObj(line.split((splitBy)));
                ProducerRecord<String, JSONObject> record = new ProducerRecord<>("test-Kafka",""+lineIndex,attack);
                producer.send(record);
                System.out.println("Record sent with index "+record);
                lineIndex++;
                Thread.sleep(150);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
    }
    static JSONObject createAttackObj(String[] dataArray){
        JSONObject atkObj=new JSONObject();
        atkObj.put("main_category",dataArray[0]);
        atkObj.put("sub_category",dataArray[1]);
        atkObj.put("protocol",dataArray[2]);
        atkObj.put("source_port",dataArray[3]);
        atkObj.put("destination_port",dataArray[4]);
        atkObj.put("name",dataArray[5]);

        return atkObj;
    }

}

