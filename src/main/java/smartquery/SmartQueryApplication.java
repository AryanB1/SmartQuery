package smartquery;

import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.ingest.kafka.KafkaIngestConsumer;
import smartquery.ingest.http.HttpIngestController;
import smartquery.ingest.model.Event;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Main application class for SmartQuery database.
 * This is a simplified version that demonstrates the core functionality
 * without full Spring Boot web server integration.
 */
public class SmartQueryApplication {
    
    private final ColumnStore columnStore;
    private final IngestService ingestService;
    private final KafkaIngestConsumer kafkaConsumer;
    private final HttpIngestController httpController;
    
    public SmartQueryApplication() {
        // Initialize components
        this.columnStore = new ColumnStore();
        this.ingestService = new IngestService(columnStore);
        this.kafkaConsumer = new KafkaIngestConsumer(ingestService);
        this.httpController = new HttpIngestController(ingestService);
        
        // Start Kafka consumer
        kafkaConsumer.start();
    }
    
    public static void main(String[] args) {
        SmartQueryApplication app = new SmartQueryApplication();
        app.run();
    }
    
    public void run() {
        System.out.println("SmartQuery Database Starting...");
        System.out.println("Ingest Service: Ready");
        System.out.println("Column Store: Ready");
        System.out.println("Kafka Consumer: Started");
        
        // Demonstrate functionality with some test data
        demonstrateIngestion();
        
        // Keep the application running
        try {
            System.out.println("\nSmartQuery is running. Press Ctrl+C to stop.");
            
            // Print stats every 10 seconds
            while (true) {
                Thread.sleep(10000);
                printStats();
            }
        } catch (InterruptedException e) {
            System.out.println("\nShutting down...");
            shutdown();
        }
    }
    
    private void demonstrateIngestion() {
        System.out.println("\n--- Demonstrating Event Ingestion ---");
        
        // Create some sample events
        List<Event> testEvents = Arrays.asList(
            new Event("user123", "login").addProperty("ip", "192.168.1.1"),
            new Event("user456", "page_view").addProperty("page", "/dashboard"),
            new Event("user123", "click").addProperty("button", "submit"),
            new Event("user789", "purchase").addProperty("amount", "29.99")
        );
        
        // Submit via HTTP controller simulation
        String result = httpController.ingestEvents(testEvents);
        System.out.println("HTTP Ingestion Result: " + result);
        
        // Print initial stats
        printStats();
    }
    
    private void printStats() {
        Map<String, Object> stats = httpController.getStats();
        System.out.println("\n--- System Stats ---");
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        System.out.println("Health: " + httpController.getHealth());
    }
    
    private void shutdown() {
        kafkaConsumer.stop();
        ingestService.stop();
        System.out.println("SmartQuery shutdown complete.");
    }
    
    // Getters for testing
    public ColumnStore getColumnStore() { return columnStore; }
    public IngestService getIngestService() { return ingestService; }
    public HttpIngestController getHttpController() { return httpController; }
}
