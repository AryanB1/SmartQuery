package smartquery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.query.QueryService;
import smartquery.index.IndexManager;
import smartquery.metrics.MetricsCollector;
import smartquery.ingest.model.Event;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

class SmartQueryApplicationTest {

    private SmartQueryApplication app;

    @BeforeEach
    void setUp() {
        // Create app with default config (no synthetic data)
        SmartQueryApplication.Config config = new SmartQueryApplication.Config();
        config.loadSyntheticData = false;
        config.syntheticDataCount = 0;
        config.metricsReportIntervalSeconds = 3600; // Long interval for tests
        app = new SmartQueryApplication(config);
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.shutdown();
        }
    }

    @Test
    void testApplicationInitialization() {
        assertNotNull(app.getColumnStore());
        assertNotNull(app.getIngestService());
        assertNotNull(app.getQueryService());
        assertNotNull(app.getIndexManager());
        assertNotNull(app.getMetrics());
    }

    @Test
    void testComponentIntegration() {
        ColumnStore store = app.getColumnStore();
        IngestService ingestService = app.getIngestService();
        QueryService queryService = app.getQueryService();

        // Verify components are properly connected
        assertNotNull(store);
        assertNotNull(ingestService);
        assertNotNull(queryService);

        // Test that they can work together
        List<Event> events = Arrays.asList(
            new Event("testUser", "testEvent")
        );

        int submitted = ingestService.submit(events);
        assertEquals(1, submitted);

        // Force flush and verify storage
        ingestService.flush();
        assertEquals(1, store.size());
        assertEquals(1, queryService.getTotalEventCount());
    }

    @Test
    void testEndToEndWorkflow() {
        IngestService ingestService = app.getIngestService();
        QueryService queryService = app.getQueryService();
        
        // Create test events
        List<Event> events = Arrays.asList(
            new Event("user1", "login").addProperty("ip", "192.168.1.1"),
            new Event("user2", "page_view").addProperty("page", "/dashboard"),
            new Event("user1", "logout")
        );

        // Ingest events
        int submitted = ingestService.submit(events);
        assertEquals(3, submitted);

        // Check initial stats
        Map<String, Object> stats = ingestService.stats();
        assertNotNull(stats);
        
        // Force flush and verify final state
        ingestService.flush();
        assertEquals(3, app.getColumnStore().size());
        assertEquals(3, queryService.getTotalEventCount());
    }

    @Test
    void testStatsIntegration() {
        IngestService ingestService = app.getIngestService();
        
        // Initially should have zero events
        Map<String, Object> initialStats = ingestService.stats();
        @SuppressWarnings("unchecked")
        Map<String, Object> initialStoreStats = (Map<String, Object>) initialStats.get("store");
        assertEquals(0L, initialStoreStats.get("totalEvents"));

        // Add some events
        List<Event> events = Arrays.asList(
            new Event("user1", "action1"),
            new Event("user2", "action2")
        );
        ingestService.submit(events);
        ingestService.flush();

        // Stats should reflect the new events
        Map<String, Object> updatedStats = ingestService.stats();
        @SuppressWarnings("unchecked")
        Map<String, Object> updatedStoreStats = (Map<String, Object>) updatedStats.get("store");
        assertEquals(2L, updatedStoreStats.get("totalEvents"));
    }

    @Test
    void testMetricsIntegration() {
        MetricsCollector metrics = app.getMetrics();
        
        // Test counter - use proper smartquery_ prefix
        var counter = metrics.incCounter("smartquery_test_counter");
        counter.inc();
        assertEquals(1, counter.value());
        
        // Test histogram
        var histogram = metrics.getHistogram("smartquery_test_histogram");
        histogram.observe(100);
        assertEquals(1, histogram.count());
        assertEquals(100.0, histogram.sum());
        
        // Test gauge
        var gauge = metrics.setGauge("smartquery_test_gauge");
        gauge.set(42);
        assertEquals(42.0, gauge.value());
    }

    @Test
    void testQueryServiceIntegration() {
        IngestService ingestService = app.getIngestService();
        QueryService queryService = app.getQueryService();
        
        // Add test data
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "login"),
            new Event("user1", "logout")
        );
        
        ingestService.submit(events);
        ingestService.flush();
        
        // Test basic functionality
        assertEquals(3, queryService.getTotalEventCount());
        assertTrue(queryService.getTableNames().contains("events"));
        
        Map<String, Object> storageStats = queryService.getStorageStats();
        assertNotNull(storageStats);
        assertEquals(3L, storageStats.get("totalEvents"));
    }

    @Test
    void testIndexManagerIntegration() {
        IndexManager indexManager = app.getIndexManager();
        
        // Test basic functionality
        Map<String, Object> stats = indexManager.stats();
        assertNotNull(stats);
        assertEquals(0, stats.get("totalIndexes"));
        
        // Register a segment
        indexManager.registerSegment("test_table", "segment1", 100);
        
        // Verify segment was registered
        Map<String, Object> updatedStats = indexManager.stats();
        assertEquals(1, updatedStats.get("totalSegments"));
    }

    @Test
    void testInvalidEventHandling() {
        IngestService ingestService = app.getIngestService();
        
        // Test with invalid events (null and empty events)
        List<Event> invalidEvents = Arrays.asList(
            new Event("user1", "valid_event"),
            new Event("user2", null),  // invalid
            new Event("user3", "")     // invalid
        );

        // IngestService doesn't filter invalid events in this implementation
        // It accepts all events and lets the storage layer handle them
        int submitted = ingestService.submit(invalidEvents);
        assertEquals(3, submitted); // All events are submitted, validation happens elsewhere
    }

    @Test
    void testEmptyEventHandling() {
        IngestService ingestService = app.getIngestService();
        
        int result1 = ingestService.submit(null);
        assertEquals(0, result1); // Empty list, no events to submit
        
        int result2 = ingestService.submit(Arrays.asList());
        assertEquals(0, result2); // No events to submit
    }

    @Test
    void testMultipleTableSupport() {
        IngestService ingestService = app.getIngestService();
        QueryService queryService = app.getQueryService();
        
        // Create events for different tables
        Event userEvent = new Event("user1", "login");
        userEvent.table = "users";
        
        Event orderEvent = new Event("user1", "purchase");
        orderEvent.table = "orders";
        
        Event defaultEvent = new Event("user1", "click"); // uses default "events" table

        List<Event> events = Arrays.asList(userEvent, orderEvent, defaultEvent);
        
        int submitted = ingestService.submit(events);
        assertEquals(3, submitted);
        
        ingestService.flush();
        
        // Verify events are stored
        assertEquals(3, app.getColumnStore().size());
        List<String> tableNames = queryService.getTableNames();
        assertEquals(3, tableNames.size());
        assertTrue(tableNames.contains("users"));
        assertTrue(tableNames.contains("orders"));
        assertTrue(tableNames.contains("events"));
    }
}
