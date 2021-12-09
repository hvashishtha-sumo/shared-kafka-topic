package hack1;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ProcessingPool {
    private final ExecutorService pool = Executors.newCachedThreadPool();

    // pool.
    // splits the tasks -- based on id --, and makes task/id and submits it ot the pool
    // ensures, not more than N tasks /id are submitted
    // task returns anything? task is complete -- it has corresponding ConsumerRecord.
    // on completion, it 'updates' that it is complete... in offset manager.
    //

    private Queue<ConsumerRecord<String, SRecord>> perCustomerQ = new LinkedBlockingQueue<>();
    private static Map<Integer, Queue<ConsumerRecord<String, SRecord>>> customerQueueM = new ConcurrentHashMap<>();
    public static void processPartitionRecords(List<ConsumerRecord<String, SRecord>> records) {
        // create customer level q.
        for (ConsumerRecord<String, SRecord> rec: records) {
            if(customerQueueM.containsKey(rec.value().getId())) {
                customerQueueM.get(rec.value().getId()).add(rec);
            } else {
                Queue<ConsumerRecord<String, SRecord>> customerQ = new LinkedBlockingQueue<>();
                customerQ.add(rec);
                customerQueueM.put(rec.value().getId(), customerQ);
            }
        }
        //poller thread, to poll all the queues and submit the task to the pool.

    }

}
