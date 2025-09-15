package smartquery.api;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import smartquery.metrics.MetricsCollector;
import smartquery.ingest.IngestService;
import smartquery.query.QueryService;
import smartquery.index.IndexManager;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000")
public class MetricsController {
    
    private final MetricsCollector metrics;
    private final IngestService ingestService;
    private final QueryService queryService;
    private final IndexManager indexManager;
    
    public MetricsController(MetricsCollector metrics, IngestService ingestService, 
                           QueryService queryService, IndexManager indexManager) {
        this.metrics = metrics;
        this.ingestService = ingestService;
        this.queryService = queryService;
        this.indexManager = indexManager;
    }
    
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getMetrics() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Query metrics
            var queryHistogram = metrics.getHistogram("smartquery_query_duration_ms");
            var successCounter = metrics.incCounter("smartquery_queries_success");
            var failedCounter = metrics.incCounter("smartquery_queries_failed");
            
            long totalQueries = queryHistogram.count();
            double averageQueryTime = totalQueries > 0 ? queryHistogram.sum() / totalQueries : 0;
            
            // Storage metrics
            long totalEvents = queryService.getTotalEventCount();
            
            // Index metrics
            Map<String, Object> indexStats = indexManager.stats();
            
            // Memory metrics
            Runtime runtime = Runtime.getRuntime();
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            long maxMemory = runtime.maxMemory();
            double memoryUsagePercent = (double) usedMemory / maxMemory;
            
            // CPU metrics (simplified)
            double systemLoad = getSystemLoad();
            
            response.put("totalEvents", totalEvents);
            response.put("queriesExecuted", totalQueries);
            response.put("averageQueryTime", averageQueryTime);
            response.put("memoryUsage", usedMemory);
            response.put("memoryUsagePercent", memoryUsagePercent);
            response.put("systemLoad", systemLoad);
            response.put("indexCount", indexStats.get("totalIndexes"));
            response.put("success", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            response.put("success", false);
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    @GetMapping("/metrics/detailed")
    public ResponseEntity<Map<String, Object>> getDetailedMetrics() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Ingest stats
            Map<String, Object> ingestStats = ingestService.stats();
            response.put("ingest", ingestStats);
            
            // Index stats
            Map<String, Object> indexStats = indexManager.stats();
            response.put("index", indexStats);
            
            // Metrics snapshot
            Map<String, Object> metricsSnapshot = metrics.snapshot();
            response.put("metrics", metricsSnapshot);
            
            // JVM stats
            Runtime runtime = Runtime.getRuntime();
            Map<String, Object> jvmStats = new HashMap<>();
            jvmStats.put("usedMemory", runtime.totalMemory() - runtime.freeMemory());
            jvmStats.put("freeMemory", runtime.freeMemory());
            jvmStats.put("totalMemory", runtime.totalMemory());
            jvmStats.put("maxMemory", runtime.maxMemory());
            response.put("jvm", jvmStats);
            
            response.put("success", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            response.put("success", false);
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    private double getSystemLoad() {
        // Simple system load approximation
        // In a real implementation, you might use OperatingSystemMXBean
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        return (double) usedMemory / maxMemory;
    }
}