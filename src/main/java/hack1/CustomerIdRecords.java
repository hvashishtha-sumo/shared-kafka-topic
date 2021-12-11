package hack1;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

// data read from kafka, and its cid
public class CustomerIdRecords {
    private final int cid;
    private final List<ConsumerRecord<String, SRecord>> records;

    CustomerIdRecords(final int cid, final List<ConsumerRecord<String, SRecord>> records) {
        this.cid = cid;
        this.records = records;
    }

    // TODO keep a way to know what offsets are processed.
    // For now, we will assume either entire batch passes, or fails.

    long minOffset() {
        return this.records.get(0).offset();
    }

    long maxOffset() {
        return this.records.get(this.records.size() -1).offset();
    }

    int recordsSize() {
        return records.size();
    }
    int cid() {
        return cid;
    }

    public List<ConsumerRecord<String, SRecord>> getRecords() {
        return records;
    }
}
