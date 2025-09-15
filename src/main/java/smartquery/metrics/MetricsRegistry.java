package smartquery.metrics;

import smartquery.metrics.MetricsCollector.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for managing and organizing metrics.
 * Provides Prometheus text format exposition and metric name validation.
 */
public class MetricsRegistry {
    
    private final Map<String, Counter> counters = new ConcurrentHashMap<>();
    private final Map<String, Gauge> gauges = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final Map<String, long[]> histogramBuckets = new ConcurrentHashMap<>();
    
    private final boolean enabled = Boolean.parseBoolean(
        System.getProperty("smartquery.metrics.enabled", "true"));
    
    /**
     * Get or create a counter.
     */
    public Counter getOrCreateCounter(String name) {
        if (!enabled) {
            return NoOpCounter.INSTANCE;
        }
        
        validateMetricName(name);
        return counters.computeIfAbsent(name, k -> new CounterImpl());
    }
    
    /**
     * Get or create a gauge.
     */
    public Gauge getOrCreateGauge(String name) {
        if (!enabled) {
            return NoOpGauge.INSTANCE;
        }
        
        validateMetricName(name);
        return gauges.computeIfAbsent(name, k -> new GaugeImpl());
    }
    
    /**
     * Get or create a histogram.
     */
    public Histogram getOrCreateHistogram(String name, long[] buckets) {
        if (!enabled) {
            return NoOpHistogram.INSTANCE;
        }
        
        validateMetricName(name);
        
        // Store buckets for later reference
        histogramBuckets.put(name, buckets);
        
        return histograms.computeIfAbsent(name, k -> new HistogramImpl(buckets));
    }
    
    /**
     * Get snapshot of all metrics as a map.
     */
    public Map<String, Object> snapshot() {
        Map<String, Object> snapshot = new HashMap<>();
        
        // Counters
        Map<String, Long> counterSnapshot = new HashMap<>();
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            counterSnapshot.put(entry.getKey(), entry.getValue().value());
        }
        snapshot.put("counters", counterSnapshot);
        
        // Gauges
        Map<String, Double> gaugeSnapshot = new HashMap<>();
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            gaugeSnapshot.put(entry.getKey(), entry.getValue().value());
        }
        snapshot.put("gauges", gaugeSnapshot);
        
        // Histograms
        Map<String, Map<String, Object>> histogramSnapshot = new HashMap<>();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            Histogram hist = entry.getValue();
            Map<String, Object> histData = new HashMap<>();
            histData.put("count", hist.count());
            histData.put("sum", hist.sum());
            histData.put("buckets", hist.buckets());
            histData.put("bucketCounts", hist.bucketCounts());
            histogramSnapshot.put(entry.getKey(), histData);
        }
        snapshot.put("histograms", histogramSnapshot);
        
        return snapshot;
    }
    
    /**
     * Render all metrics in Prometheus text format.
     */
    public String scrapePrometheus() {
        StringBuilder sb = new StringBuilder();
        
        // Counters
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            String name = entry.getKey();
            Counter counter = entry.getValue();
            
            sb.append("# TYPE ").append(name).append(" counter\n");
            sb.append(name).append(" ").append(counter.value()).append("\n");
        }
        
        // Gauges
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            String name = entry.getKey();
            Gauge gauge = entry.getValue();
            
            sb.append("# TYPE ").append(name).append(" gauge\n");
            sb.append(name).append(" ").append(gauge.value()).append("\n");
        }
        
        // Histograms
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            String name = entry.getKey();
            Histogram hist = entry.getValue();
            long[] buckets = hist.buckets();
            long[] bucketCounts = hist.bucketCounts();
            
            sb.append("# HELP ").append(name).append(" Histogram\n");
            sb.append("# TYPE ").append(name).append(" histogram\n");
            
            // Bucket counts
            for (int i = 0; i < buckets.length; i++) {
                sb.append(name).append("_bucket{le=\"").append(buckets[i]).append("\"} ")
                  .append(bucketCounts[i]).append("\n");
            }
            
            // +Inf bucket
            sb.append(name).append("_bucket{le=\"+Inf\"} ")
              .append(bucketCounts[bucketCounts.length - 1]).append("\n");
            
            // Count and sum
            sb.append(name).append("_count ").append(hist.count()).append("\n");
            sb.append(name).append("_sum ").append(hist.sum()).append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Validate metric name follows SmartQuery conventions.
     */
    private void validateMetricName(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Metric name cannot be null or empty");
        }
        
        // Check for smartquery prefix
        if (!name.startsWith("smartquery_")) {
            throw new IllegalArgumentException(
                "Metric name must start with 'smartquery_': " + name);
        }
        
        // Check for valid characters (alphanumeric and underscore)
        if (!name.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException(
                "Metric name contains invalid characters: " + name);
        }
    }
    
    /**
     * Get all registered metric names.
     */
    public Set<String> getMetricNames() {
        Set<String> names = new HashSet<>();
        names.addAll(counters.keySet());
        names.addAll(gauges.keySet());
        names.addAll(histograms.keySet());
        return names;
    }
    
    /**
     * Clear all metrics (useful for testing).
     */
    public void clear() {
        counters.clear();
        gauges.clear();
        histograms.clear();
        histogramBuckets.clear();
    }
    
    /**
     * Get registry statistics.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("enabled", enabled);
        stats.put("counterCount", counters.size());
        stats.put("gaugeCount", gauges.size());
        stats.put("histogramCount", histograms.size());
        return stats;
    }
    
    // No-op implementations for when metrics are disabled
    
    private static class NoOpCounter implements Counter {
        static final NoOpCounter INSTANCE = new NoOpCounter();
        
        @Override public void inc() {}
        @Override public void add(long value) {}
        @Override public long value() { return 0; }
    }
    
    private static class NoOpGauge implements Gauge {
        static final NoOpGauge INSTANCE = new NoOpGauge();
        
        @Override public void set(long value) {}
        @Override public void set(double value) {}
        @Override public void inc() {}
        @Override public void dec() {}
        @Override public double value() { return 0.0; }
    }
    
    private static class NoOpHistogram implements Histogram {
        static final NoOpHistogram INSTANCE = new NoOpHistogram();
        private static final long[] EMPTY_BUCKETS = new long[0];
        
        @Override public void observe(long value) {}
        @Override public long count() { return 0; }
        @Override public double sum() { return 0.0; }
        @Override public long[] buckets() { return EMPTY_BUCKETS; }
        @Override public long[] bucketCounts() { return EMPTY_BUCKETS; }
    }
}