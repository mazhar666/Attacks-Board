package kafkaConsumer;

public interface IKafkaConstants {
    String KAFKA_BROKERS = "localhost:9092";

    String CLIENT_ID="client-1";
    Integer MESSAGE_COUNT=1000;
    Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    String TOPIC_NAME= "testKafka";

}
