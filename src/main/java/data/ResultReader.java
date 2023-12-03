package data;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import proto.RecordProto;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ResultReader {

    private static final String OUTPUT_TOPIC = "output";

    public static void main(String[] args) throws Exception {
        var topPar = new TopicPartition(OUTPUT_TOPIC, 0);
        var consumer = createKafkaConsumer(topPar);

        var endOffset = consumer.endOffsets(List.of(topPar)).get(topPar);
        while (consumer.position(topPar) < endOffset) {
            var recs = consumer.poll(Duration.ofMillis(100));
            for (var rec : recs) {
                RecordProto.RecordInfo result = RecordProto.RecordInfo.parseFrom(rec.value());

                String dateTime = result.getDateTime();
                String date = dateTime.substring(0, 8);
                int hour = Integer.parseInt(dateTime.substring(8));
                String interval = String.format("%s %d:00 - %s %d:00", date, hour, date, hour + 1);

                System.out.println("User agent: " + result.getUserAgent());
                System.out.println("Record type: " + result.getType());
                System.out.println("Time interval: " + interval);
                System.out.println("Record count: " + result.getRecordCount());
                System.out.println("Average time between subsequent events: " + result.getAverageBetweenRecords());
                System.out.println();
            }
        }
    }

    private static KafkaConsumer<String, byte[]> createKafkaConsumer(TopicPartition topPar) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.assign(List.of(topPar));
        return consumer;
    }
}
