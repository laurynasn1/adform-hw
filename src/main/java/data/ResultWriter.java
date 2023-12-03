package data;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import proto.RecordProto;

import java.util.List;
import java.util.Properties;

public class ResultWriter implements AutoCloseable {

    private static final String OUTPUT_TOPIC = "output";
    private KafkaProducer<String, byte[]> producer;

    public ResultWriter() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        this.producer = new KafkaProducer<>(props);
    }

    public void writeResults(List<RecordProto.RecordInfo> results) {
        for (var result : results) {
            var record = new ProducerRecord<>(OUTPUT_TOPIC, result.getUserAgent(), result.toByteArray());
            producer.send(record);
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
