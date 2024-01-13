package Utilz.Constants;

public interface IConstants {
    String KAFKA_BROKERS = " localhost:9092";
    String CLIENT_ID="client-1";
    String INPUT_TOPIC_NAME= "attacks-input-stream";
    String OUTPUT_TOPIC_NAME= "attacks-output-stream";
    String GROUP_ID="test-group";
    String esAlerts="attack-alerts";
    String esLog="attack-log";
}
