package hack1;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class ProcessingPool {
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    private static final Logger LOG = Logger.getLogger(ProcessingPool.class.getName());

    // pool.
    // splits the tasks -- based on id --, and makes task/id and submits it ot the pool
    // ensures, not more than N tasks /id are submitted
    // task returns anything? task is complete -- it has corresponding ConsumerRecord.
    // on completion, it 'updates' that it is complete... in offset manager.
    //

    public static CompletableFuture<CustomerOffset> processRecordsPerPartition(final CustomerIdRecords customerIdRecords) {
        CompletableFuture<CustomerOffset> perCustomerCallable = createCustomerBatchCf(customerIdRecords);
        return perCustomerCallable;
    }

    private static CompletableFuture<CustomerOffset> createCustomerBatchCf(final CustomerIdRecords customerIdRecords) {
        // peek items from the customer.
    // TODO Make a max size/batch        final int maxRecPerTask = 10;
        if (customerIdRecords.recordsSize() > 0) {
            LOG.severe("Created batch for customer " + customerIdRecords.cid() + ", records batch size=" + customerIdRecords.recordsSize());
            return processBatch(customerIdRecords);
        } else {
            System.out.println("No element for cid " + customerIdRecords.cid());
            CompletableFuture<CustomerOffset> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalArgumentException("No element found for customerId " + customerIdRecords.cid()));
            return f;
        }
    }

     private static CompletableFuture<CustomerOffset> processBatch(CustomerIdRecords batch) {
        //final CompletableFuture<Void> f = new CompletableFuture<>();
        final CompletableFuture<CustomerOffset> f = new CompletableFuture<>();
            switch (batch.cid()) {
                case 1: {
                    printBatch(batch);
                    //f.completeExceptionally(new IllegalStateException("LOONG Computation Exception"));
                    LOG.severe("Processing records for customer 1");
                    f.complete(new CustomerOffset(batch.cid(), batch.minOffset()));
                    break;
                }
                default:
                    printBatch(batch);
                    f.complete(new CustomerOffset(batch.cid(), batch.maxOffset()));
//                    f.complete(null);
                    System.out.println("Processing completed for batch with id=" + batch.cid());
            }
            return f;
        }

        private static void printBatch(CustomerIdRecords batch) {
            System.out.println("Batch={id:" + batch.cid());
            System.out.println(", msgs.count=" + batch.recordsSize()+", msgs=" + batch.getRecords());
            System.out.println("}");
        }
}
