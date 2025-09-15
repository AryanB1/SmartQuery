package smartquery.metrics;

import smartquery.metrics.MetricsCollector.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the metrics system.
 */
public class MetricsTests {
    
    private MetricsCollector metricsCollector;
    private MetricsRegistry registry;
    
    @BeforeEach
    void setUp() {
        registry = new MetricsRegistry();
        metricsCollector = new MetricsCollector(registry);
        registry.clear(); // Start with clean state
    }
    
    @Test
    void testCounterBasicOperations() {
        Counter counter = metricsCollector.incCounter("smartquery_test_counter");
        
        assertEquals(0, counter.value());
        
        counter.inc();
        assertEquals(1, counter.value());
        
        counter.add(5);
        assertEquals(6, counter.value());
        
        counter.add(0);
        assertEquals(6, counter.value());
    }
    
    @Test
    void testGaugeBasicOperations() {
        Gauge gauge = metricsCollector.setGauge("smartquery_test_gauge");
        
        assertEquals(0.0, gauge.value(), 0.001);
        
        gauge.set(42L);
        assertEquals(42.0, gauge.value(), 0.001);
        
        gauge.set(3.14);
        assertEquals(3.14, gauge.value(), 0.001);
        
        gauge.inc();
        assertEquals(4.14, gauge.value(), 0.001);
        
        gauge.dec();
        assertEquals(3.14, gauge.value(), 0.001);
    }
    
    @Test
    void testHistogramBasicOperations() {
        long[] buckets = {10, 50, 100, 500, 1000};
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_histogram", buckets);
        
        assertEquals(0, histogram.count());
        assertEquals(0.0, histogram.sum(), 0.001);
        
        histogram.observe(25);
        histogram.observe(75);
        histogram.observe(200);
        
        assertEquals(3, histogram.count());
        assertEquals(300.0, histogram.sum(), 0.001);
        
        long[] bucketCounts = histogram.bucketCounts();
        assertEquals(buckets.length + 1, bucketCounts.length); // +1 for +Inf
        
        // 25 should be in 50ms bucket (cumulative)
        assertTrue(bucketCounts[1] >= 1); // 50ms bucket
        
        // All values should be in +Inf bucket
        assertEquals(3, bucketCounts[bucketCounts.length - 1]);
    }
    
    @Test
    void testHistogramBucketDistribution() {
        long[] buckets = {10, 50, 100};
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_distribution", buckets);
        
        // Add values across different buckets
        histogram.observe(5);   // <= 10
        histogram.observe(25);  // <= 50
        histogram.observe(75);  // <= 100
        histogram.observe(150); // > 100 (goes to +Inf)
        
        long[] bucketCounts = histogram.bucketCounts();
        
        // Buckets are cumulative
        assertEquals(1, bucketCounts[0]); // Values <= 10: just the 5
        assertEquals(2, bucketCounts[1]); // Values <= 50: 5 and 25
        assertEquals(3, bucketCounts[2]); // Values <= 100: 5, 25, and 75
        assertEquals(4, bucketCounts[3]); // All values (+Inf): all 4 values
    }
    
    @Test
    void testMetricsTimer() throws Exception {
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_timer");
        Counter successCounter = metricsCollector.incCounter("smartquery_test_success");
        
        // Test basic timing
        try (MetricsTimer timer = MetricsTimer.start(histogram, successCounter)) {
            Thread.sleep(10); // Small delay
        }
        
        assertEquals(1, histogram.count());
        assertTrue(histogram.sum() >= 10); // At least 10ms
        assertEquals(1, successCounter.value());
    }
    
    @Test
    void testMetricsTimerFailure() {
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_timer_fail");
        Counter successCounter = metricsCollector.incCounter("smartquery_test_success_fail");
        Counter failureCounter = metricsCollector.incCounter("smartquery_test_failure_fail");
        
        // Test failure recording
        try (MetricsTimer timer = MetricsTimer.start(histogram, successCounter, failureCounter)) {
            timer.failure();
        }
        
        assertEquals(1, histogram.count());
        assertEquals(0, successCounter.value());
        assertEquals(1, failureCounter.value());
    }
    
    @Test
    void testMetricsTimerConvenience() throws Exception {
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_convenience");
        
        // Test timing a Runnable
        MetricsTimer.time(histogram, () -> {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        
        assertEquals(1, histogram.count());
        assertTrue(histogram.sum() >= 5);
        
        // Test timing a Callable
        String result = MetricsTimer.time(histogram, () -> {
            Thread.sleep(5);
            return "test";
        });
        
        assertEquals("test", result);
        assertEquals(2, histogram.count());
    }
    
    @Test
    void testPrometheusFormat() {
        Counter counter = metricsCollector.incCounter("smartquery_test_prometheus_counter");
        Gauge gauge = metricsCollector.setGauge("smartquery_test_prometheus_gauge");
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_prometheus_histogram");
        
        counter.add(42);
        gauge.set(3.14);
        histogram.observe(100);
        
        String prometheus = metricsCollector.scrapePrometheus();
        
        // Check counter
        assertTrue(prometheus.contains("# TYPE smartquery_test_prometheus_counter counter"));
        assertTrue(prometheus.contains("smartquery_test_prometheus_counter 42"));
        
        // Check gauge
        assertTrue(prometheus.contains("# TYPE smartquery_test_prometheus_gauge gauge"));
        assertTrue(prometheus.contains("smartquery_test_prometheus_gauge 3.14"));
        
        // Check histogram
        assertTrue(prometheus.contains("# TYPE smartquery_test_prometheus_histogram histogram"));
        assertTrue(prometheus.contains("smartquery_test_prometheus_histogram_count 1"));
        assertTrue(prometheus.contains("smartquery_test_prometheus_histogram_sum 100"));
        assertTrue(prometheus.contains("_bucket{le=\"+Inf\"} 1"));
    }
    
    @Test
    @Timeout(30)
    void testThreadSafety() throws Exception {
        Counter counter = metricsCollector.incCounter("smartquery_test_thread_safety_counter");
        Gauge gauge = metricsCollector.setGauge("smartquery_test_thread_safety_gauge");
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_thread_safety_histogram");
        
        int numThreads = 10;
        int operationsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Void>> futures = new ArrayList<>();
        
        // Start concurrent operations
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    counter.inc();
                    gauge.set(threadId * operationsPerThread + j);
                    histogram.observe(j);
                }
                return null;
            }));
        }
        
        // Wait for completion
        for (Future<Void> future : futures) {
            future.get();
        }
        
        // Verify results
        assertEquals(numThreads * operationsPerThread, counter.value());
        assertEquals(numThreads * operationsPerThread, histogram.count());
        assertTrue(gauge.value() >= 0); // Gauge should have some value
        
        executor.shutdown();
    }
    
    @Test
    void testMetricsRegistryValidation() {
        // Valid names should work
        assertDoesNotThrow(() -> registry.getOrCreateCounter("smartquery_valid_name"));
        
        // Invalid names should fail
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getOrCreateCounter("invalid_name")); // Missing prefix
        
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getOrCreateCounter("smartquery_invalid-name")); // Invalid char
        
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getOrCreateCounter("")); // Empty name
        
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getOrCreateCounter(null)); // Null name
    }
    
    @Test
    void testMetricsSnapshot() {
        Counter counter = metricsCollector.incCounter("smartquery_test_snapshot_counter");
        Gauge gauge = metricsCollector.setGauge("smartquery_test_snapshot_gauge");
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_snapshot_histogram");
        
        counter.add(10);
        gauge.set(20.5);
        histogram.observe(100);
        histogram.observe(200);
        
        Map<String, Object> snapshot = metricsCollector.snapshot();
        
        assertNotNull(snapshot);
        assertTrue(snapshot.containsKey("counters"));
        assertTrue(snapshot.containsKey("gauges"));
        assertTrue(snapshot.containsKey("histograms"));
        
        @SuppressWarnings("unchecked")
        Map<String, Long> counters = (Map<String, Long>) snapshot.get("counters");
        assertEquals(Long.valueOf(10), counters.get("smartquery_test_snapshot_counter"));
        
        @SuppressWarnings("unchecked")
        Map<String, Double> gauges = (Map<String, Double>) snapshot.get("gauges");
        assertEquals(Double.valueOf(20.5), gauges.get("smartquery_test_snapshot_gauge"));
        
        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> histograms = (Map<String, Map<String, Object>>) snapshot.get("histograms");
        Map<String, Object> histData = histograms.get("smartquery_test_snapshot_histogram");
        assertEquals(Long.valueOf(2), histData.get("count"));
        assertEquals(Double.valueOf(300.0), histData.get("sum"));
    }
    
    @Test
    void testRegistryStats() {
        registry.getOrCreateCounter("smartquery_test_stats_counter1");
        registry.getOrCreateCounter("smartquery_test_stats_counter2");
        registry.getOrCreateGauge("smartquery_test_stats_gauge1");
        registry.getOrCreateHistogram("smartquery_test_stats_histogram1", new long[]{10, 100});
        
        Map<String, Object> stats = registry.stats();
        
        assertEquals(2, stats.get("counterCount"));
        assertEquals(1, stats.get("gaugeCount"));
        assertEquals(1, stats.get("histogramCount"));
        assertTrue((Boolean) stats.get("enabled"));
    }
    
    @Test
    void testMetricsDisabled() {
        // This test requires system property to be set
        // In a real test, we'd use dependency injection or test-specific registry
        System.setProperty("smartquery.metrics.enabled", "false");
        
        MetricsRegistry disabledRegistry = new MetricsRegistry();
        MetricsCollector disabledCollector = new MetricsCollector(disabledRegistry);
        
        Counter counter = disabledCollector.incCounter("smartquery_test_disabled");
        counter.inc();
        counter.add(100);
        
        // Should return 0 when disabled
        assertEquals(0, counter.value());
        
        // Clean up
        System.setProperty("smartquery.metrics.enabled", "true");
    }
    
    @Test
    void testCounterTimer() {
        Counter successCounter = metricsCollector.incCounter("smartquery_test_counter_timer_success");
        Counter failureCounter = metricsCollector.incCounter("smartquery_test_counter_timer_failure");
        
        // Test success
        try (MetricsTimer.CounterTimer timer = MetricsTimer.startCounter(successCounter, failureCounter)) {
            // Auto-success on close
        }
        
        assertEquals(1, successCounter.value());
        assertEquals(0, failureCounter.value());
        
        // Test explicit failure
        try (MetricsTimer.CounterTimer timer = MetricsTimer.startCounter(successCounter, failureCounter)) {
            timer.failure();
        }
        
        assertEquals(1, successCounter.value());
        assertEquals(1, failureCounter.value());
    }
    
    @Test
    void testHistogramEdgeCases() {
        long[] buckets = {10, 50, 100};
        Histogram histogram = metricsCollector.getHistogram("smartquery_test_edge_cases", buckets);
        
        // Test boundary values
        histogram.observe(10); // Exactly on boundary
        histogram.observe(0);  // Below all buckets
        histogram.observe(1000); // Above all buckets
        
        long[] bucketCounts = histogram.bucketCounts();
        
        // All observations should be counted in +Inf bucket
        assertEquals(3, bucketCounts[bucketCounts.length - 1]);
        assertEquals(3, histogram.count());
        assertEquals(1010.0, histogram.sum(), 0.001);
    }
    
    @Test
    void testConcurrentRegistryAccess() throws Exception {
        int numThreads = 5;
        int metricsPerThread = 20;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            futures.add(executor.submit(() -> {
                for (int j = 0; j < metricsPerThread; j++) {
                    String metricName = String.format("smartquery_test_concurrent_%d_%d", threadId, j);
                    Counter counter = registry.getOrCreateCounter(metricName);
                    counter.inc();
                }
                return null;
            }));
        }
        
        for (Future<Void> future : futures) {
            future.get();
        }
        
        // Should have created numThreads * metricsPerThread unique counters
        assertEquals(numThreads * metricsPerThread, registry.getMetricNames().size());
        
        executor.shutdown();
    }
}