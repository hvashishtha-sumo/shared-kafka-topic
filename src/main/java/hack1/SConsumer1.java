package hack1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class SConsumer1 {
    public static void main(String[] args) {
        consumeRecords();
    }
    private static void consumeRecords() {
        String topic = "test2";
        final Properties p = initProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        final ConsumerRebalanceListener listener = new PartitionListener(consumer);
        consumer.subscribe(Arrays.asList(topic), listener);
        //consumer.subscribe(Arrays.asList(topic));


        int recordsCount = 0;
        // consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
            for (TopicPartition tp: records.partitions()) {
                List<ConsumerRecord<String, String>> perPartitionRecords = records.records(tp);
                //re-create the record from values.
                List<ConsumerRecord<String, SRecord>> perPartitionData = fromString(perPartitionRecords);

            }
            for (ConsumerRecord rec: records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", rec.offset(), rec.key(), rec.value());
                // commit offset
                recordsCount++;
                /**
                 * a map of topicPartition, and a ds for offset of all 'ids' in all the records for a partition
                 * Given that there are a limited number of ids, a particular id is handled by a dedicated tp.
                 * That map is populated in the listener#onAssign, and the revoked entries are flushed on onRevoke.
                 *
                 * The value of the entry in the map are changed as data is read for that partition
                 * but the keyset should be changed only from listener.
                 *
                 */
                consumer.commitAsync();
                //consumer.assignment().
            }
            System.out.println(recordsCount + " records consumed.");
        }
    }

    // helper method to convert string values to SRecords.
    private static List<ConsumerRecord<String, SRecord>> fromString(List<ConsumerRecord<String, String>> vals) {
        List<ConsumerRecord<String, SRecord>> res = new ArrayList<>(vals.size());
        for (ConsumerRecord<String, String> s: vals) {
            String[] mems = s.value().split("#");
            if (mems.length != 3) {
                throw new IllegalArgumentException("Not valid record, " + s);
            }

            boolean add = res.add(new ConsumerRecord<String, SRecord>(
                    s.topic(), s.partition(), s.offset(), s.key(), new SRecord(Integer.parseInt(mems[1]), mems[2])));
        }
        return res;
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
