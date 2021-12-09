package hack;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SConsumer {
    public static void main(String[] args) {
        consumeRecords();
    }
    private static void consumeRecords() {
        String topic = "t1";
        final Properties p = initProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        final ConsumerRebalanceListener listener = new PartitionListener(consumer);
        consumer.subscribe(Arrays.asList(topic), listener);
        //consumer.subscribe(Arrays.asList(topic));


        int recordsCount = 0;
        // consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
            for (ConsumerRecord rec: records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", rec.offset(), rec.key(), rec.value());
                // commit offset
                recordsCount++;
                consumer.commitAsync();
                //consumer.assignment().
            }
            System.out.println(recordsCount + " records consumed.");
        }
    }

    private static Properties initProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091");
        props.put("group.id", "test1");
        //props.put("enable.auto.commit", "true");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private static class PartitionListener implements ConsumerRebalanceListener {
        private KafkaConsumer<String, String> consumer;
        PartitionListener(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            System.out.println("Revoked partition, " + collection);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            // fetch the current offset, and print.
            final Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = consumer.committed(collection.stream().collect(Collectors.toSet()));
            offsetAndMetadataMap.forEach((a, b) -> {
                if (b != null) {
                    System.out.println(a + ", offset=" + b.offset() + ", metadata=" + b.metadata());
                    consumer.seek(a, b.offset());
                } else {
                    System.out.println(a + ", and Null offset");
                }
            });
        }
    }
}

/**
 * import java.util.Properties;
 * import java.util.Arrays;
 * import org.apache.kafka.clients.consumer.KafkaConsumer;
 * import org.apache.kafka.clients.consumer.ConsumerRecords;
 * import org.apache.kafka.clients.consumer.ConsumerRecord;
 *
 * public class SimpleConsumer {
 *    public static void main(String[] args) throws Exception {
 *       if(args.length == 0){
 *          System.out.println("Enter topic name");
 *          return;
 *       }
 *       //Kafka consumer configuration settings
 *       String topicName = args[0].toString();
 *       Properties props = new Properties();
 *
 *       props.put("bootstrap.servers", "localhost:9092");
 *       props.put("group.id", "test");
 *       props.put("enable.auto.commit", "true");
 *       props.put("auto.commit.interval.ms", "1000");
 *       props.put("session.timeout.ms", "30000");
 *       props.put("key.deserializer",
 *          "org.apache.kafka.common.serializa-tion.StringDeserializer");
 *       props.put("value.deserializer",
 *          "org.apache.kafka.common.serializa-tion.StringDeserializer");
 *       KafkaConsumer<String, String> consumer = new KafkaConsumer
 *          <String, String>(props);
 *
 *       //Kafka Consumer subscribes list of topics here.
 *       consumer.subscribe(Arrays.asList(topicName))
 *
 *       //print the topic name
 *       System.out.println("Subscribed to topic " + topicName);
 *       int i = 0;
 *
 *       while (true) {
 *          ConsumerRecords<String, String> records = con-sumer.poll(100);
 *          for (ConsumerRecord<String, String> record : records)
 *
 *          // print the offset,key and value for the consumer records.
 *          System.out.printf("offset = %d, key = %s, value = %s\n",
 *             record.offset(), record.key(), record.value());
 *       }
 *    }
 * }
 */
