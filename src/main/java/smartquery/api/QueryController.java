package smartquery.api;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import smartquery.query.QueryService;
import smartquery.query.QueryApi;
import smartquery.metrics.MetricsCollector;
import smartquery.metrics.MetricsTimer;

import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:3000")
public class QueryController {
    
    private final QueryService queryService;
    private final MetricsCollector metrics;
    
    public QueryController(QueryService queryService, MetricsCollector metrics) {
        this.queryService = queryService;
        this.metrics = metrics;
    }
    
    @PostMapping("/query")
    public ResponseEntity<Map<String, Object>> executeQuery(@RequestBody QueryRequest request) {
        if (request.sql == null || request.sql.trim().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "SQL query is required"
            ));
        }
        
        // Record query metrics
        var queryTimer = MetricsTimer.start(
            metrics.getHistogram("smartquery_query_duration_ms"),
            metrics.incCounter("smartquery_queries_success"),
            metrics.incCounter("smartquery_queries_failed")
        );
        
        try {
            long startTime = System.currentTimeMillis();
            
            // Execute query
            QueryApi.QueryResult result = queryService.executeQuery(request.sql.trim());
            
            long executionTime = System.currentTimeMillis() - startTime;
            
            Map<String, Object> response = new HashMap<>();
            response.put("columns", result.columns);
            response.put("rows", result.rows);
            response.put("executionTime", executionTime);
            response.put("success", true);
            
            queryTimer.success();
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            queryTimer.failure();
            
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            response.put("success", false);
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    @GetMapping("/tables")
    public ResponseEntity<Map<String, Object>> getTables() {
        try {
            Map<String, Object> response = new HashMap<>();
            response.put("tables", queryService.getTableNames());
            response.put("success", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            response.put("success", false);
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    @GetMapping("/schema/{tableName}")
    public ResponseEntity<Map<String, Object>> getTableSchema(@PathVariable String tableName) {
        try {
            // For now, return a simple schema - this would be enhanced in a real implementation
            Map<String, Object> response = new HashMap<>();
            response.put("tableName", tableName);
            response.put("columns", Map.of(
                "userId", "STRING",
                "eventType", "STRING", 
                "timestamp", "TIMESTAMP",
                "properties", "MAP"
            ));
            response.put("success", true);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("error", e.getMessage());
            response.put("success", false);
            
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    public static class QueryRequest {
        public String sql;
        
        public QueryRequest() {}
        
        public QueryRequest(String sql) {
            this.sql = sql;
        }
    }
}