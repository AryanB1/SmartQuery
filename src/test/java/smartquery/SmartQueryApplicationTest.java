package smartquery;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import smartquery.storage.ColumnStore;
import smartquery.ingest.IngestService;
import smartquery.ingest.http.HttpIngestController;
import smartquery.ingest.model.Event;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

class SmartQueryApplicationTest {

    private SmartQueryApplication app;

    @BeforeEach
    void setUp() {
        app = new SmartQueryApplication();
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            // The application doesn't have a public shutdown method
            // The components will be cleaned up by garbage collection
            app.getIngestService().stop();
        }
    }

    @Test
    void testApplicationInitialization() {
        assertNotNull(app.getColumnStore());
        assertNotNull(app.getIngestService());
        assertNotNull(app.getHttpController());
    }

    @Test
    void testComponentIntegration() {
        ColumnStore store = app.getColumnStore();
        IngestService ingestService = app.getIngestService();
        HttpIngestController controller = app.getHttpController();

        // Verify components are properly connected
        assertNotNull(store);
        assertNotNull(ingestService);
        assertNotNull(controller);

        // Test that they can work together
        List<Event> events = Arrays.asList(
            new Event("testUser", "testEvent")
        );

        String result = controller.ingestEvents(events);
        assertTrue(result.contains("success"));

        // Force flush and verify storage
        ingestService.flush();
        assertEquals(1, store.size());
    }

    @Test
    void testEndToEndWorkflow() {
        HttpIngestController controller = app.getHttpController();
        
        // Create test events
        List<Event> events = Arrays.asList(
            new Event("user1", "login").addProperty("ip", "192.168.1.1"),
            new Event("user2", "page_view").addProperty("page", "/dashboard"),
            new Event("user1", "logout")
        );

        // Ingest events
        String result = controller.ingestEvents(events);
        assertTrue(result.contains("success"));
        assertTrue(result.contains("accepted 3 events"));

        // Check stats
        Map<String, Object> stats = controller.getStats();
        assertNotNull(stats);
        
        // Check health
        String health = controller.getHealth();
        assertEquals("healthy", health);

        // Force flush and verify final state
        app.getIngestService().flush();
        assertEquals(3, app.getColumnStore().size());
    }

    @Test
    void testStatsIntegration() {
        HttpIngestController controller = app.getHttpController();
        
        // Initially should have zero events
        Map<String, Object> initialStats = controller.getStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> initialStoreStats = (Map<String, Object>) initialStats.get("store");
        assertEquals(0L, initialStoreStats.get("totalEvents"));

        // Add some events
        List<Event> events = Arrays.asList(
            new Event("user1", "action1"),
            new Event("user2", "action2")
        );
        controller.ingestEvents(events);
        app.getIngestService().flush();

        // Stats should reflect the new events
        Map<String, Object> updatedStats = controller.getStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> updatedStoreStats = (Map<String, Object>) updatedStats.get("store");
        assertEquals(2L, updatedStoreStats.get("totalEvents"));
    }

    @Test
    void testHealthCheckIntegration() {
        HttpIngestController controller = app.getHttpController();
        
        // Should start healthy
        assertEquals("healthy", controller.getHealth());
        
        // Should remain healthy after normal operations
        List<Event> events = Arrays.asList(
            new Event("user1", "test_event")
        );
        controller.ingestEvents(events);
        assertEquals("healthy", controller.getHealth());
    }

    @Test
    void testInvalidEventHandling() {
        HttpIngestController controller = app.getHttpController();
        
        // Test with invalid events
        List<Event> invalidEvents = Arrays.asList(
            new Event("user1", "valid_event"),
            new Event("user2", null),  // invalid
            new Event("user3", "")     // invalid
        );

        String result = controller.ingestEvents(invalidEvents);
        assertTrue(result.startsWith("error:"));
        assertTrue(result.contains("2 invalid events"));
    }

    @Test
    void testEmptyEventHandling() {
        HttpIngestController controller = app.getHttpController();
        
        String result1 = controller.ingestEvents(null);
        assertEquals("error: empty event list", result1);
        
        String result2 = controller.ingestEvents(Arrays.asList());
        assertEquals("error: empty event list", result2);
    }

    @Test
    void testMultipleTableSupport() {
        HttpIngestController controller = app.getHttpController();
        
        // Create events for different tables
        Event userEvent = new Event("user1", "login");
        userEvent.table = "users";
        
        Event orderEvent = new Event("user1", "purchase");
        orderEvent.table = "orders";
        
        Event defaultEvent = new Event("user1", "click"); // uses default "events" table

        List<Event> events = Arrays.asList(userEvent, orderEvent, defaultEvent);
        
        String result = controller.ingestEvents(events);
        assertTrue(result.contains("success"));
        
        app.getIngestService().flush();
        
        // Verify events are stored
        assertEquals(3, app.getColumnStore().size());
        assertEquals(3, app.getColumnStore().getTableNames().size());
        assertTrue(app.getColumnStore().getTableNames().contains("users"));
        assertTrue(app.getColumnStore().getTableNames().contains("orders"));
        assertTrue(app.getColumnStore().getTableNames().contains("events"));
    }
}
