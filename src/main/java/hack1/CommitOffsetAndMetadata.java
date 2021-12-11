package hack1;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class CommitOffsetAndMetadata {
    private static final Logger LOG = Logger.getLogger(CommitOffsetAndMetadata.class.getName());
    private long offset;
    private final Map<Integer, Long> custOffsets;

    CommitOffsetAndMetadata(long offset, Map<Integer, Long> offsetsMap) {
        this.offset = offset;
        this.custOffsets = offsetsMap;
    }

    public static CommitOffsetAndMetadata fromCustomerOffsets(List<CustomerOffset> offsets) {
        final AtomicLong minOffset = new AtomicLong(Integer.MAX_VALUE);
        final Map<Integer, Long> offsetMap = new HashMap<>();
        offsets.forEach(a -> {
            if (minOffset.get() > a.offset) {
                minOffset.set(a.offset);
            }
            offsetMap.put(a.customerId, a.offset);
        });
        return new CommitOffsetAndMetadata(minOffset.get(), offsetMap);
    }

    public long getOffset() {
        return offset;
    }

    public String offsetMdString() {
        final StringBuilder sb = new StringBuilder();
        custOffsets.forEach((a, b) -> sb.append(a).append("-").append(b.longValue()).append(","));
        return sb.toString();
    }

    public void updateOrAddCustomerOffset(final CustomerOffset customerOffset ) {
        if (custOffsets.containsKey(customerOffset.customerId)) {
            LOG.info("Found existing offset for customer="+ customerOffset.customerId + ", updating it. Oldoffset=" +
                    custOffsets.get(customerOffset.customerId) + ", newOffset=" + customerOffset) ;
        }
        custOffsets.put(customerOffset.customerId, customerOffset.offset);
        if (this.offset > customerOffset.offset) {
            this.offset = customerOffset.offset;
        }
    }

    public long offsetForCustomer(final int cid) {
        return custOffsets.getOrDefault(cid, -1L);
    }
    public static CommitOffsetAndMetadata fromOffsetAndMetadata(final OffsetAndMetadata ofmd) {
        // sample: 0-248,1-239,
        // ignore the last comma.
        final String md = ofmd.metadata();
        final String md1 = md.substring(0, md.length() - 1);
        final String[] mems = md1.split(",");
        final Map<Integer, Long> offsetMap = new HashMap<>();
        for (int i = 0; i < mems.length; i++) {
            String[] entry = mems[i].split("-");
            offsetMap.put(Integer.parseInt(entry[0]), Long.parseLong(entry[1]));
        }
        return new CommitOffsetAndMetadata(ofmd.offset(), offsetMap);
    }

    @Override
    public String toString() {
        return "CommitOffsetAndMetadata{" +
                "offset=" + offset +
                ", custOffsets=" + custOffsets +
                '}';
    }
}
