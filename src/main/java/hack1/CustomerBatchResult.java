package hack1;

import java.util.function.Function;

// represents the result for processing a batch of message for a customer
public class CustomerBatchResult {
    private CustomerIdRecords recordsBatch;
    private boolean success;

    public static final Function<CustomerIdRecords, CustomerOffset> INVALID_RESULT =
            customerIdRecords -> new CustomerOffset(customerIdRecords.cid(), -1L);

    CustomerBatchResult(CustomerIdRecords recs, boolean success) {
        this.recordsBatch = recs;
        this.success = success;
    }

    public CustomerOffset offset() {
        if(success) {
            return new CustomerOffset(recordsBatch.cid(), recordsBatch.maxOffset());
        } else {
            return INVALID_RESULT.apply(recordsBatch);
        }
    }
}
