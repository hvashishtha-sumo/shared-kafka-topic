package hack1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CommitOffsetAndMetadata {
    private final long offset;
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

    @Override
    public String toString() {
        return "CommitOffsetAndMetadata{" +
                "offset=" + offset +
                ", custOffsets=" + custOffsets +
                '}';
    }
}
