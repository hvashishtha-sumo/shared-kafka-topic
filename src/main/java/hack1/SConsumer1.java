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
        String topic = CommonConfiguration.topic;
        final Properties p = initProps();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        final ConsumerRebalanceListener listener = new PartitionListener(consumer);
        consumer.subscribe(Arrays.asList(topic), listener);
        //consumer.subscribe(Arrays.asList(topic));
        int recordsCount = 0;
        // consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3_000));
            final Map<TopicPartition, OffsetAndMetadata> tpOffsetMd = new HashMap<>();
            for (TopicPartition tp: records.partitions()) {
                List<ConsumerRecord<String, String>> perPartitionRecords = records.records(tp);
                //re-create the record from values.
                Map<TopicPartition, OffsetAndMetadata> currentOffsetAndMd = consumer.committed(Collections.singleton(tp));
                List<ConsumerRecord<String, SRecord>> perPartitionData = fromString(perPartitionRecords);
                tpOffsetMd.put(tp, processPartitionRecords(perPartitionData, currentOffsetAndMd.get(tp)));
                recordsCount += perPartitionData.size();
            }
            consumer.commitSync(tpOffsetMd);
            LOG.info("Records consumed = " + recordsCount);
        }
    }

    //TODO Refactor value to have more data, such as min/max offset, etc
    private static Map<Integer, BlockingQueue<ConsumerRecord<String, SRecord>>> customerQueueM = new ConcurrentHashMap<>();

    private static final int TIMEOUT_MS = 30_000;
    private static OffsetAndMetadata processPartitionRecords(List<ConsumerRecord<String, SRecord>> partitionRecords,
                                                             final OffsetAndMetadata currentCommittedOffsetAndMd) {
        // create customer level q.
        LOG.info("Processing " + partitionRecords.size() + " data from a partition");
        for (ConsumerRecord<String, SRecord> rec: partitionRecords) {
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
        List<CompletableFuture<CustomerBatchResult>> customerFutures = new LinkedList<>();
        final CommitOffsetAndMetadata previousCommittedOffsetAndMetadata = CommitOffsetAndMetadata.fromOffsetAndMetadata(currentCommittedOffsetAndMd);
        // filter out the data that has already been processed.
        customerQueueM.forEach((a, b) -> {
            // create ConsumerRecords
            final List<ConsumerRecord<String, SRecord>> recs = new LinkedList<>();
            if (previousCommittedOffsetAndMetadata != null) {
                final long currentCommittedOffset = previousCommittedOffsetAndMetadata.offsetForCustomer(a);
                if (currentCommittedOffset > 0) {
                    // there is some offset from the previous commit, check and ignore already processed records
                    while (b.peek() != null && b.peek().offset() <= currentCommittedOffset) {
                        ConsumerRecord<String, SRecord> curRecord = b.remove();
                        LOG.info("Skipping processing record for customer " + a + " currentCommittedOffset=" +
                                currentCommittedOffset + ", readRecordOffset=" + curRecord.offset());
                    }
                }
            }
            if (b.peek() != null) {
                LOG.info("Start to process records for customer " + a + " from offset " + b.peek().offset());
                // drain remaining elems in the list for processing
                b.drainTo(recs);
                final CustomerIdRecords customerIdRecords = new CustomerIdRecords(a, recs);
                CompletableFuture<CustomerBatchResult> custFuture = ProcessingPool.processRecordsPerPartition(customerIdRecords);
                customerFutures.add(custFuture);
            } else {
                //TODO: Can't remove the old md as one need to keep it up. BUG-XXX
                LOG.severe("No elements to process for the customer " + a);
            }
        });
        FutureUtils.waitForFutures(customerFutures, TIMEOUT_MS);
        // scan future results, and create a offset datastructure.
        List<CustomerOffset> processedOffsets = new LinkedList<>();
        customerFutures.forEach((a) -> {
            // All futures are completed; the ones with exceptions are returning the min offset from the batch.
            CustomerBatchResult result = FutureUtils.getValueForCompletedCfAndSwallowAnyException(a, 2_000);
            processedOffsets.add(result.offset());
        });
        LOG.info("processedOffsets:" + processedOffsets);
        // a ds to represent the combined offset map. Populate it and commit with it.
        // update the commitOffsetAndMD with the new batch updates
        LOG.info("Old commitOffsetAndMd=" + previousCommittedOffsetAndMetadata);

        final CommitOffsetAndMetadata offsetAndMdForThisBatch = CommitOffsetAndMetadata.fromCustomerOffsets(processedOffsets);
        LOG.info("New batch commitOffsetAndMd=" + offsetAndMdForThisBatch);
        // merge old and new ofmd.
        final CommitOffsetAndMetadata merged = CommitOffsetAndMetadata.merge(offsetAndMdForThisBatch, previousCommittedOffsetAndMetadata);
//        if (previousCommittedOffsetAndMetadata != null) {
//            processedOffsets.forEach(a -> previousCommittedOffsetAndMetadata.updateOrAddCustomerOffset(a));
//            merged = previousCommittedOffsetAndMetadata;
//        } else {
//            merged = offsetAndMdForThisBatch;
//        }
        LOG.info("Merged commitOffsetAndMd=" + merged);
        // commit
        final OffsetAndMetadata ofmd = new OffsetAndMetadata(merged.getOffset(), merged.offsetMdString());
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
                    LOG.info("Assigned topic " + a + ", offset=" + b.offset() + ", metadata=" + b.metadata());
                    consumer.seek(a, b.offset());
                } else {
                    LOG.info("Assigned topic " + a + ", with Null offset");
                }
            });
        }
    }
}
