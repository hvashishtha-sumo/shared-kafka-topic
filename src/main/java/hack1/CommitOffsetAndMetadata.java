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

    public static CommitOffsetAndMetadata merge(final CommitOffsetAndMetadata first, final CommitOffsetAndMetadata sec) {
        if (first == null && sec == null) {
            return null;
        } else if (first == null) {
            return sec;
        } else if (sec == null) {
            return first;
        }
        // -ve offset check.
        if (first.offset < 0) {
            return sec;
        } else if (sec.offset < 0) {
            return first;
        }

        final CommitOffsetAndMetadata res = new CommitOffsetAndMetadata(-1, new HashMap<>());
        if (first.offset < sec.offset) {
            res.offset = first.offset;
        } else {
            res.offset = sec.offset;
        }
        res.custOffsets.putAll(first.custOffsets);
        for (Map.Entry<Integer, Long> e: sec.custOffsets.entrySet()) {
            if (res.custOffsets.containsKey(e.getKey())) {
                // check which one is higher, and use that.
                final long min = Math.max(res.custOffsets.get(e.getKey()), e.getValue());
                res.custOffsets.put(e.getKey(), min);
            } else {
                // new key, add it
                res.custOffsets.put(e.getKey(), e.getValue());
            }
        }
        return res;
    }



    public static CommitOffsetAndMetadata fromCustomerOffsets(List<CustomerOffset> offsets) {
        final AtomicLong minOffset = new AtomicLong(Long.MAX_VALUE);
        final Map<Integer, Long> offsetMap = new HashMap<>();
        offsets.forEach(a -> {
            // ignore invalid offsets.
            if (a.offset > 0) {
                if (minOffset.get() > a.offset) {
                    minOffset.set(a.offset);
                }
                offsetMap.put(a.customerId, a.offset);
            } else {
                LOG.warning("Ignoring invalid offset " + a);
            }
        });
        if (minOffset.get() == Long.MAX_VALUE) {
            // return null
            return null;
        }
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
        if (customerOffset.offset < 0) {
            LOG.severe("Not updating the offset as the processing failed for the current batch, offset=" + customerOffset);
            return;
        }
        custOffsets.put(customerOffset.customerId, customerOffset.offset);
        // recompute offset
        this.offset = Long.MAX_VALUE;
        custOffsets.forEach((a, b) -> {
            if (this.offset > b) {
                this.offset = b;
            }
        });
    }


    public long offsetForCustomer(final int cid) {
        return custOffsets.getOrDefault(cid, -1L);
    }
    public static CommitOffsetAndMetadata fromOffsetAndMetadata(final OffsetAndMetadata ofmd) {
        if (ofmd == null) {
            return null;
        }
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
