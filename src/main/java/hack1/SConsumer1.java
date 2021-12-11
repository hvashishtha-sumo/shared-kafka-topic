package hack1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class SConsumer1 {
    private static final Logger LOG = Logger.getLogger(SConsumer1.class.getName());
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
//            consumer.committed()
            final Map<TopicPartition, OffsetAndMetadata> tpOffsetMd = new HashMap<>();
            for (TopicPartition tp: records.partitions()) {
                List<ConsumerRecord<String, String>> perPartitionRecords = records.records(tp);
                //re-create the record from values.
                List<ConsumerRecord<String, SRecord>> perPartitionData = fromString(perPartitionRecords);
                tpOffsetMd.put(tp, processPartitionRecords(perPartitionData));
                recordsCount += perPartitionData.size();
            }
            consumer.commitSync(tpOffsetMd);
//            for (ConsumerRecord rec: records) {
//                System.out.printf("offset = %d, key = %s, value = %s\n", rec.offset(), rec.key(), rec.value());
//                // commit offset
//                recordsCount++;
//                /**
//                 * a map of topicPartition, and a ds for offset of all 'ids' in all the records for a partition
//                 * Given that there are a limited number of ids, a particular id is handled by a dedicated tp.
//                 * That map is populated in the listener#onAssign, and the revoked entries are flushed on onRevoke.
//                 *
//                 * The value of the entry in the map are changed as data is read for that partition
//                 * but the keyset should be changed only from listener.
//                 *
//                 */
//
//                consumer.commitAsync();
//                //consumer.assignment().
//            }
            LOG.severe("Records consumed = " + recordsCount);
            System.out.println(recordsCount + " records consumed.");
        }
    }

    //TODO Refactor value to have more data, such as min/max offset, etc
    private static Map<Integer, BlockingQueue<ConsumerRecord<String, SRecord>>> customerQueueM = new ConcurrentHashMap<>();

    private static final int TIMEOUT_MS = 30_000;
    private static OffsetAndMetadata processPartitionRecords(List<ConsumerRecord<String, SRecord>> records) {
        // create customer level q.
        LOG.severe("Processing " + records.size() + " data from a partition");
        for (ConsumerRecord<String, SRecord> rec: records) {
            if(customerQueueM.containsKey(rec.value().getId())) {
                customerQueueM.get(rec.value().getId()).add(rec);
            } else {
                BlockingQueue<ConsumerRecord<String, SRecord>> customerQ = new LinkedBlockingQueue<>();
                customerQ.add(rec);
                customerQueueM.put(rec.value().getId(), customerQ);
            }
        }
        LOG.severe("Consumer queue size=" + customerQueueM.size());

        //Map<Integer, CompletableFuture<CustomerOffset>> customerFutures = new HashMap<>();
        List<CompletableFuture<CustomerOffset>> customerFutures = new LinkedList<>();
        customerQueueM.forEach((a, b) -> {
            // create ConsumerRecords
            final List<ConsumerRecord<String, SRecord>> recs = new LinkedList<>();
            b.drainTo(recs);
            final CustomerIdRecords customerIdRecords = new CustomerIdRecords(a, recs);
            CompletableFuture<CustomerOffset> custFuture = ProcessingPool.processRecordsPerPartition(customerIdRecords);
            customerFutures.add(custFuture);
        });
        FutureUtils.waitForFutures(customerFutures, TIMEOUT_MS);
        // scan future results, and create a offset datastructure.
        List<CustomerOffset> processedOffsets = new LinkedList<>();
        customerFutures.forEach((a) -> {
            // All futures are completed; the ones with exceptions are returning the min offset from the batch.
            processedOffsets.add(FutureUtils.getValueForCompletedCfAndSwallowAnyException(a, 2_000));
        });
        LOG.info("processedOffsets:" + processedOffsets);
        LOG.severe("processedOffsets=" + processedOffsets);
        // a ds to represent the combined offset map. Populate it and commit with it.
        final CommitOffsetAndMetadata offsetAndMd = CommitOffsetAndMetadata.fromCustomerOffsets(processedOffsets);
        // commit
        final OffsetAndMetadata ofmd = new OffsetAndMetadata(offsetAndMd.getOffset(), offsetAndMd.offsetMdString());
        LOG.info("OffsetAndMD=" + ofmd);
        return ofmd;
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
        props.put("group.id", CommonConfiguration.GROUP_ID);
        //props.put("enable.auto.commit", "true");
        props.setProperty("enable.auto.commit", "false");
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
