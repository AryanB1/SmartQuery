package smartquery.metrics;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

/**
 * HTTP controller for exposing metrics in Prometheus format.
 * Provides /metrics endpoint compatible with Prometheus scraping.
 */
@RestController
@RequestMapping("/metrics")
public class MetricsHttpController {
    
    private final MetricsCollector metricsCollector;
    
    /**
     * Constructor with dependency injection.
     */
    public MetricsHttpController(MetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
    
    /**
     * Default constructor that creates its own metrics collector.
     */
    public MetricsHttpController() {
        this.metricsCollector = new MetricsCollector();
    }
    
    /**
     * Main metrics endpoint in Prometheus text format.
     */
    @GetMapping(produces = "text/plain;charset=UTF-8")
    public ResponseEntity<String> metrics() {
        try {
            String prometheusText = metricsCollector.scrapePrometheus();
            return ResponseEntity.ok()
                .contentType(MediaType.TEXT_PLAIN)
                .body(prometheusText);
        } catch (Exception e) {
            // Return error metrics
            return ResponseEntity.status(500)
                .contentType(MediaType.TEXT_PLAIN)
                .body("# Error generating metrics: " + e.getMessage() + "\n");
        }
    }
    
    /**
     * JSON format endpoint for debugging.
     */
    @GetMapping(value = "/json", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> metricsJson() {
        try {
            Map<String, Object> snapshot = metricsCollector.snapshot();
            return ResponseEntity.ok(snapshot);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = Map.of(
            "status", "UP",
            "timestamp", System.currentTimeMillis(),
            "metrics_enabled", System.getProperty("smartquery.metrics.enabled", "true")
        );
        return ResponseEntity.ok(health);
    }
}