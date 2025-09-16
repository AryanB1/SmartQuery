package smartquery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.beans.factory.annotation.Autowired;
import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.query.QueryService;
import smartquery.index.IndexManager;
import smartquery.metrics.MetricsCollector;
import smartquery.metrics.MetricsTimer;
import smartquery.query.QueryApi;
import smartquery.ingest.model.Event;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main Spring Boot application class that provides both web API and console interface.
 */
@SpringBootApplication
public class SmartQueryApplication implements CommandLineRunner {
    
    // Core components (injected by Spring)
    @Autowired
    private ColumnStore columnStore;
    
    @Autowired
    private IngestService ingestService;
    
    @Autowired
    private QueryService queryService;
    
    @Autowired
    private IndexManager indexManager;
    
    @Autowired
    private MetricsCollector metrics;
    
    // Background services
    private final ScheduledExecutorService metricsScheduler;
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    // Configuration (will be set from command line args)
    private boolean loadSyntheticData = false;
    private int syntheticDataCount = 10_000;
    private int metricsReportIntervalSeconds = 30;
    
    
    public SmartQueryApplication() {
        // Spring will inject dependencies
        this.metricsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics");
            t.setDaemon(true);
            return t;
        });
    }
    
    
    public static void main(String[] args) {
        SpringApplication.run(SmartQueryApplication.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--synthetic-data":
                    this.loadSyntheticData = true;
                    if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        this.syntheticDataCount = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--metrics-interval":
                    if (i + 1 < args.length) {
                        this.metricsReportIntervalSeconds = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
            }
        }
        
        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
        
    System.out.println("\nSmartQuery is now running.");
        
        try {
            runConsoleMode();
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void runConsoleMode() {
        // Display startup banner
        displayStartupBanner();
        
        // Start background services
        startBackgroundServices();
        
        // Load synthetic data if requested
        if (loadSyntheticData) {
            loadSyntheticData();
        }
        
        // Start console REPL
        startConsoleRepl();
    }
    
    
    private void displayStartupBanner() {
        System.out.println("SmartQuery Database Engine v1.0");
        System.out.println("Configuration: " + getIngestBatchSize() + " events/batch, " + 
                          metricsReportIntervalSeconds + "s metrics, " + 
                          (loadSyntheticData ? syntheticDataCount + " synthetic events" : "no synthetic data"));
        System.out.println();
    }
    
    
    private void startBackgroundServices() {
        metricsScheduler.scheduleAtFixedRate(
            this::reportMetrics,
            metricsReportIntervalSeconds,
            metricsReportIntervalSeconds,
            TimeUnit.SECONDS
        );
    }
    
    private void loadSyntheticData() {
        long startTime = System.currentTimeMillis();
        List<Event> events = generateSyntheticEvents(syntheticDataCount);
        int submitted = ingestService.submit(events);
        ingestService.flush();
        long loadTime = System.currentTimeMillis() - startTime;
        
        System.out.println("Loaded " + submitted + " events in " + loadTime + "ms");
        displayDataSummary();
    }
    
    
    private List<Event> generateSyntheticEvents(int count) {
        List<Event> events = new ArrayList<>();
        Random random = new Random(42); // Fixed seed for reproducible data
        
        String[] userIds = {"user001", "user002", "user003", "user004", "user005", 
                           "user006", "user007", "user008", "user009", "user010"};
        String[] eventTypes = {"login", "logout", "page_view", "click", "purchase", "signup"};
        String[] pages = {"/home", "/dashboard", "/profile", "/settings", "/checkout", "/product"};
        String[] browsers = {"Chrome", "Firefox", "Safari", "Edge"};
        String[] countries = {"US", "UK", "CA", "DE", "FR", "JP", "AU"};
        
        long baseTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7); // Start from 7 days ago
        
        for (int i = 0; i < count; i++) {
            String userId = userIds[random.nextInt(userIds.length)];
            String eventType = eventTypes[random.nextInt(eventTypes.length)];
            
            Event event = new Event(userId, eventType);
            event.ts = baseTime + random.nextInt((int) TimeUnit.DAYS.toMillis(7)); // Random time in last 7 days
            
            // Set table based on event type
            if (eventType.equals("purchase")) {
                event.table = "transactions";
                event.addProperty("amount", String.valueOf(random.nextInt(1000) + 10));
                event.addProperty("currency", "USD");
            } else {
                event.table = "user_events";
            }
            
            // Add common properties
            event.addProperty("browser", browsers[random.nextInt(browsers.length)]);
            event.addProperty("country", countries[random.nextInt(countries.length)]);
            event.addProperty("ip", "192.168." + random.nextInt(256) + "." + random.nextInt(256));
            
            // Add event-specific properties
            if (eventType.equals("page_view") || eventType.equals("click")) {
                event.addProperty("page", pages[random.nextInt(pages.length)]);
            }
            
            events.add(event);
        }
        
        return events;
    }
    
    private void displayDataSummary() {
        Map<String, Object> stats = ingestService.stats();
        System.out.println("Tables: " + queryService.getTableNames() + ", Events: " + stats.get("store"));
    }
    
    private void startConsoleRepl() {
    System.out.println("SQL Console (type 'help' for commands, 'exit' to quit):");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        while (running.get()) {
            try {
                System.out.print("smartquery> ");
                System.out.flush(); // Ensure prompt is displayed immediately
                String input = reader.readLine();
                
                if (input == null) {
                    // EOF reached (e.g., Ctrl+D or piped input ended)
                    break;
                }
                
                if (input.trim().isEmpty()) {
                    continue;
                }
                
                String command = input.trim().toLowerCase();
                
                // Handle special commands
                if (command.equals("exit") || command.equals("quit")) {
                    break;
                } else if (command.equals("help")) {
                    displayHelp();
                    continue;
                } else if (command.equals("stats")) {
                    displayDetailedStats();
                    continue;
                } else if (command.equals("tables")) {
                    displayTables();
                    continue;
                } else if (command.equals("clear")) {
                    clearScreen();
                    continue;
                }
                
                // Execute SQL query
                executeQuery(input.trim());
                
            } catch (IOException e) {
                System.err.println("Error reading input: " + e.getMessage());
                break;
            } catch (Exception e) {
                System.err.println("Unexpected error: " + e.getMessage());
            }
        }
        
    // Session ended
    }
    
    private void executeQuery(String sql) {
        if (sql.isEmpty()) return;
        
        // Record query metrics
        var queryTimer = MetricsTimer.start(
            metrics.getHistogram("smartquery_query_duration_ms"),
            metrics.incCounter("smartquery_queries_success"),
            metrics.incCounter("smartquery_queries_failed")
        );
        
        try {
            long startTime = System.currentTimeMillis();
            
            // Execute query
            QueryApi.QueryResult result = queryService.executeQuery(sql);
            
            long executionTime = System.currentTimeMillis() - startTime;
            
            // Display results
            displayQueryResult(result, executionTime);
            
            queryTimer.success();
            
        } catch (Exception e) {
            System.err.println("Query failed: " + e.getMessage());
            queryTimer.failure();
        }
    }
    
    private void displayQueryResult(QueryApi.QueryResult result, long executionTime) {
        if (result.rows.isEmpty()) {
            System.out.println("No rows returned.");
        } else {
            // Display column headers
            System.out.println();
            for (int i = 0; i < result.columns.size(); i++) {
                System.out.printf("%-20s", result.columns.get(i));
                if (i < result.columns.size() - 1) System.out.print(" | ");
            }
            System.out.println();
            
            // Display separator
            for (int i = 0; i < result.columns.size(); i++) {
                System.out.print("--------------------");
                if (i < result.columns.size() - 1) System.out.print("-+-");
            }
            System.out.println();
            
            // Display data rows (limit to first 100 for readability)
            int displayRows = Math.min(result.rows.size(), 100);
            for (int i = 0; i < displayRows; i++) {
                List<Object> row = result.rows.get(i);
                for (int j = 0; j < row.size(); j++) {
                    Object value = row.get(j);
                    String displayValue = value != null ? value.toString() : "NULL";
                    if (displayValue.length() > 18) {
                        displayValue = displayValue.substring(0, 15) + "...";
                    }
                    System.out.printf("%-20s", displayValue);
                    if (j < row.size() - 1) System.out.print(" | ");
                }
                System.out.println();
            }
            
            if (result.rows.size() > displayRows) {
                System.out.println("... (" + (result.rows.size() - displayRows) + " more rows)");
            }
        }
        
        System.out.println();
        System.out.println("Returned " + result.rows.size() + " rows in " + executionTime + "ms");
        System.out.println();
    }
    
    private void displayHelp() {
        System.out.println();
        System.out.println("--- SmartQuery Help ---");
        System.out.println("SQL Commands:");
        System.out.println("  SELECT * FROM table_name");
        System.out.println("  SELECT column1, column2 FROM table_name WHERE condition");
        System.out.println("  SELECT COUNT(*) FROM table_name");
        System.out.println("  SELECT column1, COUNT(*) FROM table_name GROUP BY column1");
        System.out.println();
        System.out.println("Console Commands:");
        System.out.println("  help    - Show this help");
        System.out.println("  stats   - Show detailed system statistics");
        System.out.println("  tables  - List available tables");
        System.out.println("  clear   - Clear the screen");
        System.out.println("  exit    - Exit SmartQuery");
        System.out.println();
        System.out.println("Example Queries:");
        System.out.println("  SELECT * FROM user_events LIMIT 10");
        System.out.println("  SELECT userId, COUNT(*) FROM user_events GROUP BY userId");
        System.out.println("  SELECT * FROM transactions WHERE amount > 100");
        System.out.println();
    }
    
    private void displayDetailedStats() {
        System.out.println();
        System.out.println("--- Detailed System Statistics ---");
        
        // Ingest stats
        Map<String, Object> ingestStats = ingestService.stats();
        System.out.println("Ingest Service:");
        System.out.println("  Buffer size: " + ingestStats.get("bufferSize"));
        System.out.println("  Dropped events: " + ingestStats.get("dropped"));
        System.out.println("  Batch size: " + ingestStats.get("batchSize"));
        System.out.println("  Flush interval: " + ingestStats.get("flushMillis") + "ms");
        
        // Storage stats
        @SuppressWarnings("unchecked")
        Map<String, Object> storageStats = (Map<String, Object>) ingestStats.get("store");
        System.out.println("Storage:");
        System.out.println("  Total events: " + storageStats.get("totalEvents"));
        System.out.println("  Total batches: " + storageStats.get("totalBatches"));
        System.out.println("  Tables count: " + storageStats.get("tablesCount"));
        
        // Index stats
        Map<String, Object> indexStats = indexManager.stats();
        System.out.println("Indexes:");
        System.out.println("  Total indexes: " + indexStats.get("totalIndexes"));
        System.out.println("  Memory usage: " + formatBytes((Long) indexStats.get("memoryBytes")));
        System.out.println("  Memory budget: " + indexStats.get("memoryBudgetMB") + " MB");
        
        // Metrics stats
        Map<String, Object> metricsSnapshot = metrics.snapshot();
        System.out.println("Metrics:");
        for (Map.Entry<String, Object> entry : metricsSnapshot.entrySet()) {
            if (entry.getKey().startsWith("smartquery_")) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue());
            }
        }
        
        // JVM stats
        Runtime runtime = Runtime.getRuntime();
        System.out.println("JVM:");
        System.out.println("  Used memory: " + formatBytes(runtime.totalMemory() - runtime.freeMemory()));
        System.out.println("  Free memory: " + formatBytes(runtime.freeMemory()));
        System.out.println("  Total memory: " + formatBytes(runtime.totalMemory()));
        System.out.println("  Max memory: " + formatBytes(runtime.maxMemory()));
        
        System.out.println();
    }
    
    private void displayTables() {
        System.out.println();
        System.out.println("--- Available Tables ---");
        List<String> tables = queryService.getTableNames();
        if (tables.isEmpty()) {
            System.out.println("No tables found. Load some data first.");
        } else {
            for (String table : tables) {
                long count = queryService.getTotalEventCount(); // This is total, would need per-table count
                System.out.println("  " + table + " (events loaded)");
            }
        }
        System.out.println();
    }
    
    private void clearScreen() {
        // ANSI escape code to clear screen
        System.out.print("\033[2J\033[H");
        System.out.flush();
    }
    
    private void reportMetrics() {
        if (!running.get()) return;
        
        System.out.println();
        System.out.println("--- Metrics Report [" + new Date() + "] ---");
        
        // Query metrics
        var queryHistogram = metrics.getHistogram("smartquery_query_duration_ms");
        var successCounter = metrics.incCounter("smartquery_queries_success");
        var failedCounter = metrics.incCounter("smartquery_queries_failed");
        
        if (queryHistogram.count() > 0) {
            System.out.println("Queries:");
            System.out.println("  Total: " + queryHistogram.count());
            System.out.println("  Success: " + successCounter.value());
            System.out.println("  Failed: " + failedCounter.value());
            System.out.println("  Avg duration: " + String.format("%.1f", queryHistogram.sum() / queryHistogram.count()) + "ms");
        }
        
        // Storage metrics
        long totalEvents = queryService.getTotalEventCount();
        System.out.println("Storage:");
        System.out.println("  Total events: " + totalEvents);
        System.out.println("  Tables: " + queryService.getTableNames().size());
        
        // Index metrics
        Map<String, Object> indexStats = indexManager.stats();
        System.out.println("Indexes:");
        System.out.println("  Count: " + indexStats.get("totalIndexes"));
        System.out.println("  Memory: " + formatBytes((Long) indexStats.get("memoryBytes")));
        
        // Memory metrics
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("JVM Memory:");
        System.out.println("  Used: " + formatBytes(usedMemory));
        System.out.println("  Free: " + formatBytes(runtime.freeMemory()));
        System.out.println("  Utilization: " + String.format("%.1f", 100.0 * usedMemory / runtime.totalMemory()) + "%");
        
        System.out.println();
        System.out.print("smartquery> "); // Restore prompt
    }
    
    public void shutdown() {
        running.set(false);
        
        System.out.println("Stopping background services...");
        
        // Stop metrics reporting
        metricsScheduler.shutdown();
        
        // Flush any remaining data
        System.out.println("Flushing remaining data...");
        ingestService.flush();
        
        // Stop services
        System.out.println("Stopping ingest service...");
        ingestService.stop();
        
        System.out.println("Stopping index manager...");
        indexManager.shutdown();
        
        // Wait for background tasks to complete
        try {
            if (!metricsScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                metricsScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            metricsScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("SmartQuery shutdown complete.");
    }
    
    private int getIngestBatchSize() {
        return Integer.getInteger("ingest.batchSize", 10_000);
    }
    
    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }
    
    private static void printUsage() {
        System.out.println("SmartQuery Database Engine");
        System.out.println();
        System.out.println("Usage: java -jar smartquery.jar [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --synthetic-data [count]    Load synthetic test data (default: 10000 events)");
        System.out.println("  --metrics-interval <sec>    Metrics reporting interval (default: 30 seconds)");
        System.out.println("  --help                      Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java -jar smartquery.jar");
        System.out.println("  java -jar smartquery.jar --synthetic-data 50000");
        System.out.println("  java -jar smartquery.jar --synthetic-data --metrics-interval 60");
    }
    
    // Getters for testing
    public ColumnStore getColumnStore() { return columnStore; }
    public IngestService getIngestService() { return ingestService; }
    public QueryService getQueryService() { return queryService; }
    public IndexManager getIndexManager() { return indexManager; }
    public MetricsCollector getMetrics() { return metrics; }
}
