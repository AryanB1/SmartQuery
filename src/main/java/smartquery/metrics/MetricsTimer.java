package smartquery.metrics;

import smartquery.metrics.MetricsCollector.Histogram;
import smartquery.metrics.MetricsCollector.Counter;

/**
 * RAII-style timing helper for measuring latencies.
 */
public class MetricsTimer implements AutoCloseable {
    
    private final long startTime;
    private final Histogram histogram;
    private final Counter successCounter;
    private final Counter failureCounter;
    private boolean completed = false;
    
    private MetricsTimer(Histogram histogram, Counter successCounter, Counter failureCounter) {
        this.startTime = System.currentTimeMillis();
        this.histogram = histogram;
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
    }
    
    public static MetricsTimer start(Histogram histogram) {
        return new MetricsTimer(histogram, null, null);
    }
    
    public static MetricsTimer start(Histogram histogram, Counter successCounter) {
        return new MetricsTimer(histogram, successCounter, null);
    }
    
    public static MetricsTimer start(Histogram histogram, Counter successCounter, Counter failureCounter) {
        return new MetricsTimer(histogram, successCounter, failureCounter);
    }
    
    public long elapsed() {
        return System.currentTimeMillis() - startTime;
    }
    
    public void success() {
        if (!completed) {
            long elapsed = elapsed();
            histogram.observe(elapsed);
            if (successCounter != null) {
                successCounter.inc();
            }
            completed = true;
        }
    }
    
    public void failure() {
        if (!completed) {
            long elapsed = elapsed();
            histogram.observe(elapsed);
            if (failureCounter != null) {
                failureCounter.inc();
            }
            completed = true;
        }
    }
    
    public void complete(boolean success) {
        if (success) {
            success();
        } else {
            failure();
        }
    }
    
    @Override
    public void close() {
        if (!completed) {
            success();
        }
    }
    
    /**
     * Convenience method to time a Runnable.
     */
    public static void time(Histogram histogram, Runnable operation) {
        try (MetricsTimer timer = start(histogram)) {
            operation.run();
        }
    }
    
    /**
     * Convenience method to time a Runnable with success/failure counting.
     */
    public static void time(Histogram histogram, Counter successCounter, Counter failureCounter, Runnable operation) {
        try (MetricsTimer timer = start(histogram, successCounter, failureCounter)) {
            operation.run();
        } catch (Exception e) {
            // Timer will automatically record failure on exception
            throw e;
        }
    }
    
    /**
     * Convenience method to time a callable operation.
     */
    public static <T> T time(Histogram histogram, java.util.concurrent.Callable<T> operation) throws Exception {
        try (MetricsTimer timer = start(histogram)) {
            return operation.call();
        }
    }
    
    /**
     * Convenience method to time a callable with success/failure counting.
     */
    public static <T> T time(Histogram histogram, Counter successCounter, Counter failureCounter, 
                           java.util.concurrent.Callable<T> operation) throws Exception {
        try (MetricsTimer timer = start(histogram, successCounter, failureCounter)) {
            T result = operation.call();
            timer.success();
            return result;
        } catch (Exception e) {
            // Timer will automatically record failure
            throw e;
        }
    }
    
    /**
     * Create a timer that only increments counters (no histogram).
     */
    public static CounterTimer startCounter(Counter successCounter, Counter failureCounter) {
        return new CounterTimer(successCounter, failureCounter);
    }
    
    /**
     * Timer that only tracks success/failure counts without latency.
     */
    public static class CounterTimer implements AutoCloseable {
        private final Counter successCounter;
        private final Counter failureCounter;
        private boolean completed = false;
        
        private CounterTimer(Counter successCounter, Counter failureCounter) {
            this.successCounter = successCounter;
            this.failureCounter = failureCounter;
        }
        
        public void success() {
            if (!completed && successCounter != null) {
                successCounter.inc();
                completed = true;
            }
        }
        
        public void failure() {
            if (!completed && failureCounter != null) {
                failureCounter.inc();
                completed = true;
            }
        }
        
        public void complete(boolean success) {
            if (success) {
                success();
            } else {
                failure();
            }
        }
        
        @Override
        public void close() {
            if (!completed) {
                success();
            }
        }
    }
}