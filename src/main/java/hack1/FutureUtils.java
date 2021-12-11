package hack1;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class FutureUtils {
    private static final Logger LOG = Logger.getLogger(FutureUtils.class.getName());
    public static <T> boolean waitForFutures(final Collection<CompletableFuture<T>> futures,
                                         final int timeoutMS) {
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).
                    get(timeoutMS, TimeUnit.MILLISECONDS);
            LOG.info("Processed all futures succesfully, total=" + futures.size());
        } catch (final Exception e) {
            final long completed = futures.stream().filter(f -> f.isDone() && !f.isCompletedExceptionally()).
                    count();
            LOG.severe("Error waiting on futures" + ", completed=" + completed +
                    ", total=" + futures.size() + ", timeoutMS=" + timeoutMS);
            return false;
        }
        return true;
    }

    public static <T> T getValueForCompletedCfAndSwallowAnyException(final CompletableFuture<T> cf,
                                                                     final int timeoutMS) {
        try {
            return cf.get(timeoutMS, TimeUnit.MILLISECONDS);
        } catch (final Throwable ie) {
            LOG.severe("Got exception while getting cf val. " + ie );
        }
        return null;
    }
}
