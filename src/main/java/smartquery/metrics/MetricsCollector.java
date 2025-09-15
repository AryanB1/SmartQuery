package smartquery.metrics;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Thread-safe metrics collector with counters, gauges, and histograms.
 */
public class MetricsCollector {
    
    private final MetricsRegistry registry;
    
    public MetricsCollector() {
        this.registry = new MetricsRegistry();
    }
    
    public MetricsCollector(MetricsRegistry registry) {
        this.registry = registry;
    }
    
    public Counter incCounter(String name) {
        return registry.getOrCreateCounter(name);
    }
    
    public Gauge setGauge(String name) {
        return registry.getOrCreateGauge(name);
    }
    
    public Histogram getHistogram(String name) {
        return getHistogram(name, DEFAULT_BUCKETS);
    }
    
    public Histogram getHistogram(String name, long[] bucketsMillis) {
        return registry.getOrCreateHistogram(name, bucketsMillis);
    }
    
    public Map<String, Object> snapshot() {
        return registry.snapshot();
    }
    
    public String scrapePrometheus() {
        return registry.scrapePrometheus();
    }
    
    private static final long[] DEFAULT_BUCKETS = {1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};
    
    public interface Counter {
        void inc();
        void add(long value);
        long value();
    }
    
    public interface Gauge {
        void set(long value);
        void set(double value);
        void inc();
        void dec();
        double value();
    }
    
    public interface Histogram {
        void observe(long value);
        long count();
        double sum();
        long[] buckets();
        long[] bucketCounts();
    }
    
    public static class CounterImpl implements Counter {
        private final LongAdder adder = new LongAdder();
        
        @Override
        public void inc() {
            adder.increment();
        }
        
        @Override
        public void add(long value) {
            adder.add(value);
        }
        
        @Override
        public long value() {
            return adder.sum();
        }
    }
    
    public static class GaugeImpl implements Gauge {
        private final AtomicLong longValue = new AtomicLong(0);
        private volatile double doubleValue = 0.0;
        private volatile boolean isDouble = false;
        
        @Override
        public void set(long value) {
            isDouble = false;
            longValue.set(value);
        }
        
        @Override
        public void set(double value) {
            isDouble = true;
            doubleValue = value;
        }
        
        @Override
        public void inc() {
            if (isDouble) {
                doubleValue += 1.0;
            } else {
                longValue.incrementAndGet();
            }
        }
        
        @Override
        public void dec() {
            if (isDouble) {
                doubleValue -= 1.0;
            } else {
                longValue.decrementAndGet();
            }
        }
        
        @Override
        public double value() {
            return isDouble ? doubleValue : longValue.get();
        }
    }
    
    public static class HistogramImpl implements Histogram {
        private final long[] buckets;
        private final LongAdder[] bucketCounts;
        private final LongAdder count = new LongAdder();
        private final DoubleAdder sum = new DoubleAdder();
        
        public HistogramImpl(long[] buckets) {
            this.buckets = Arrays.copyOf(buckets, buckets.length);
            Arrays.sort(this.buckets);
            this.bucketCounts = new LongAdder[buckets.length + 1];
            
            for (int i = 0; i < bucketCounts.length; i++) {
                bucketCounts[i] = new LongAdder();
            }
        }
        
        @Override
        public void observe(long value) {
            count.increment();
            sum.add(value);
            
            int bucketIndex = findBucketIndex(value);
            
            for (int i = bucketIndex; i < bucketCounts.length; i++) {
                bucketCounts[i].increment();
            }
        }
        
        @Override
        public long count() {
            return count.sum();
        }
        
        @Override
        public double sum() {
            return sum.sum();
        }
        
        @Override
        public long[] buckets() {
            return Arrays.copyOf(buckets, buckets.length);
        }
        
        @Override
        public long[] bucketCounts() {
            long[] counts = new long[bucketCounts.length];
            for (int i = 0; i < bucketCounts.length; i++) {
                counts[i] = bucketCounts[i].sum();
            }
            return counts;
        }
        
        private int findBucketIndex(long value) {
            for (int i = 0; i < buckets.length; i++) {
                if (value <= buckets[i]) {
                    return i;
                }
            }
            return buckets.length;
        }
    }
}