package Consumer;

import Events.Event;
import Utilz.Constants.IConstants;
import Utilz.Deserializers.EventDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Consumer {
    protected static TypeInformation<Event> typeInfo = TypeInformation.of(Event.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Event> source = createKafkaSource(IConstants.INPUT_TOPIC_NAME, new EventDeserializer());
        DataStream<Event> logStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source", typeInfo);
        DataStream<Event> alertsStream = getThreats(logStream);
//        new thread for logging data
        Thread logsThread = new Thread(() -> {
            try {
                stream2Elastic(logStream, env, IConstants.esLog);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread alertsThread = new Thread(() -> {
            try {
                stream2Elastic(alertsStream, env, IConstants.esAlerts);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        logsThread.start();
        alertsThread.start();
        KafkaSink<String> sink =createKafkaSink(IConstants.OUTPUT_TOPIC_NAME,new SimpleStringSchema());
        alertsStream.map(Event::toString).sinkTo(sink).name("kafka-sink");

        env.execute("test");
    }

    private static <T> KafkaSource<T> createKafkaSource(String topic, DeserializationSchema<T> deserializationSchema) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(IConstants.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId("test-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(deserializationSchema)
                .build();
    }


    private static DataStream<Event> getThreats(DataStream<Event> dataStream) {
        return dataStream.filter(event -> isThreat(event));

    }

    private static boolean isThreat(Event attack) {
        AtomicInteger severityIndex = new AtomicInteger(0);
        boolean portNotSafe = isNotPortSafe(attack, severityIndex);
        boolean atkTypeUnknown = isAtkTypeUnknown(attack, severityIndex);
        checkProtocol(attack, severityIndex);
        isAuthorizedIp(attack);
        if (severityIndex.get() > 0 && (portNotSafe || atkTypeUnknown)) {
            if (severityIndex.get() == 1) {
                attack.setSeverity("Low");
            } else if (severityIndex.get() == 2) {
                attack.setSeverity("Medium");
            } else if (severityIndex.get() >= 3) {
                attack.setSeverity("High");
            }
            return true;
        }
        return false;
    }

    private static boolean isNotPortSafe(Event attack, AtomicInteger severityIndex) {
        if (Objects.equals(attack.destinationPort, "80") || Objects.equals(attack.destinationPort, "24") || Objects.equals(attack.destinationPort, "3389")) {
            severityIndex.incrementAndGet();
            return true;
        }
        return false;

    }

    ;

    private static boolean isAtkTypeUnknown(Event attack, AtomicInteger severityIndex) {
        boolean isUnknown = false;

        if (Objects.equals(attack.mainCategory, "DoS")) {
            isUnknown = true;
            severityIndex.incrementAndGet();
            if (Objects.equals(attack.subCategory, "TFTP") || Objects.equals(attack.subCategory, "SSL") || Objects.equals(attack.subCategory, "FTP")) {
                severityIndex.incrementAndGet();
            }
        } else if (Objects.equals(attack.mainCategory, "Backdoor")) {
            isUnknown = true;
            severityIndex.addAndGet(5);
        } else if (Objects.equals(attack.mainCategory, "Reconnaissance")) {
            isUnknown = true;
            severityIndex.incrementAndGet();
            if (Objects.equals(attack.subCategory, "IRC") || Objects.equals(attack.subCategory, "IMAP")) {
                severityIndex.incrementAndGet();
            }
        }

        return isUnknown;
    }

    private static void checkProtocol(Event attack, AtomicInteger severityIndex) {
        if (Objects.equals(attack.protocol, "udp")) {
            severityIndex.incrementAndGet();
        }
    }

    private static void isAuthorizedIp(Event attack) {
        if (attack.sourcePort.matches("\\b(?:192\\.|(\\d{1,3}\\.0\\.0\\.\\d{1,3})|(.*[01]$))\\b")) {
            attack.setAuthorizedIp(false);
        }
    }

    private static void stream2Elastic(DataStream<Event> stream, StreamExecutionEnvironment env, String esIndex) throws Exception {
        ElasticsearchSink<Event> esSink = new Elasticsearch7SinkBuilder<Event>()
                .setBulkFlushMaxActions(1)
                .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
                .setEmitter((element, context, indexer) -> indexer.add(createIndexRequest(esIndex, element)))
                .build();
        stream.sinkTo(esSink).name(esIndex);
        env.execute("elastic-test");

    }

    private static IndexRequest createIndexRequest(String index, Event element) {
        Map<String, String> json = objectMapper.convertValue(element, Map.class);
        int randomNumber = new Random().nextInt(10000);
        String id = Arrays.stream(element.name.split("\\s+"))
                .limit(2)
                .reduce((a, b) -> a + " " + b)
                .orElse("") + randomNumber;
        IndexRequest request = Requests.indexRequest()
                .index(index)
                .id(id)
                .source(json);
        if (Objects.equals(index, IConstants.esAlerts)) {
            System.out.println(" req sent :::" + request);
        }
        return request;
    }

    private static <T> KafkaSink<T> createKafkaSink(String topic, SerializationSchema<T> serializationSchema ) {

        return KafkaSink.<T>builder().setBootstrapServers(IConstants.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(serializationSchema)
                        .build()
                )
                .build();
    }
}
