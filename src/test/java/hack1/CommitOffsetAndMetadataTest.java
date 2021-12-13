package hack1;

import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class CommitOffsetAndMetadataTest {
    private static final Logger LOG = Logger.getLogger(CommitOffsetAndMetadataTest.class.getName());

    @Test
    public void testMerge() {
        final Map<Integer, Long> m1map = new HashMap<>();
        m1map.put(1, 10l);
        m1map.put(2, 100l);
        final CommitOffsetAndMetadata m1 = new CommitOffsetAndMetadata(12, m1map);

        final Map<Integer, Long> m2map = new HashMap<>();
        m2map.put(1, 3l);
        m2map.put(2, 1000l);
        final CommitOffsetAndMetadata m2 = new CommitOffsetAndMetadata(112, m2map);

        final CommitOffsetAndMetadata m3 = CommitOffsetAndMetadata.merge(m1, m2);
        LOG.info(m3.toString());



    }
}