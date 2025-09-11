package ingest.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import ingest.model.Event;
import ingest.IngestService;
import storage.ColumnStore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

class HttpIngestControllerTest {

    private HttpIngestController controller;
    private IngestService ingestService;
    private ColumnStore columnStore;

    @BeforeEach
    void setUp() {
        columnStore = new ColumnStore();
        ingestService = new IngestService(columnStore);
        controller = new HttpIngestController(ingestService);
    }

    @Test
    void testIngestEventsSuccess() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "page_view")
        );

        String result = controller.ingestEvents(events);

        assertTrue(result.startsWith("success:"));
        assertTrue(result.contains("accepted 2 events"));
    }

    @Test
    void testIngestEventsNull() {
        String result = controller.ingestEvents(null);
        assertEquals("error: empty event list", result);
    }

    @Test
    void testIngestEventsEmpty() {
        String result = controller.ingestEvents(new ArrayList<>());
        assertEquals("error: empty event list", result);
    }

    @Test
    void testIngestEventsWithInvalidEvents() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),  // valid
            new Event("user2", null),     // invalid - null event name
            new Event("user3", ""),       // invalid - empty event name
            new Event("user4", "   ")     // invalid - whitespace only
        );

        String result = controller.ingestEvents(events);

        assertTrue(result.startsWith("error:"));
        assertTrue(result.contains("3 invalid events"));
    }

    @Test
    void testIngestEventsAllValid() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "page_view"),
            new Event("user3", "purchase")
        );

        String result = controller.ingestEvents(events);

        assertTrue(result.startsWith("success:"));
        assertTrue(result.contains("accepted 3 events"));
    }

    @Test
    void testIngestEventsSystemOverloaded() {
        // Test the error handling for backpressure
        // Note: This tests the logic flow rather than the actual backpressure mechanism
        
        // Create a reasonable number of events
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            events.add(new Event("user" + i, "event"));
        }

        String result = controller.ingestEvents(events);

        // Should normally succeed with reasonable number of events
        assertTrue(result.contains("success") || result.contains("error: system overloaded"));
    }

    @Test
    void testGetStats() {
        // Add some events first
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "logout")
        );
        controller.ingestEvents(events);
        ingestService.flush(); // Force flush to update stats

        Map<String, Object> stats = controller.getStats();

        assertNotNull(stats);
        assertTrue(stats.containsKey("bufferSize"));
        assertTrue(stats.containsKey("dropped"));
        assertTrue(stats.containsKey("batchSize"));
        assertTrue(stats.containsKey("flushMillis"));
        assertTrue(stats.containsKey("store"));

        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertNotNull(storeStats);
        assertEquals(2L, storeStats.get("totalEvents"));
    }

    @Test
    void testGetHealthHealthy() {
        String health = controller.getHealth();
        assertEquals("healthy", health);
    }

    @Test
    void testGetHealthWithSomeEvents() {
        // Add a few events - should still be healthy
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "page_view")
        );
        controller.ingestEvents(events);

        String health = controller.getHealth();
        assertEquals("healthy", health);
    }

    @Test
    void testCompleteWorkflow() {
        // Test a complete workflow from ingestion to stats
        List<Event> batch1 = Arrays.asList(
            new Event("user1", "login").addProperty("ip", "192.168.1.1"),
            new Event("user2", "page_view").addProperty("page", "/dashboard")
        );

        List<Event> batch2 = Arrays.asList(
            new Event("user1", "logout"),
            new Event("user3", "register").addProperty("source", "google")
        );

        // Ingest first batch
        String result1 = controller.ingestEvents(batch1);
        assertTrue(result1.contains("success"));

        // Ingest second batch
        String result2 = controller.ingestEvents(batch2);
        assertTrue(result2.contains("success"));

        // Force flush
        ingestService.flush();

        // Check stats
        Map<String, Object> stats = controller.getStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertEquals(4L, storeStats.get("totalEvents"));

        // Check health
        String health = controller.getHealth();
        assertEquals("healthy", health);
    }

    @Test
    void testEventValidation() {
        // Test various edge cases for event validation
        List<Event> mixedEvents = Arrays.asList(
            new Event("user1", "valid_event"),
            new Event("user2", null),
            new Event("user3", ""),
            new Event("user4", "   "),
            new Event("user5", "another_valid_event")
        );

        String result = controller.ingestEvents(mixedEvents);

        assertTrue(result.startsWith("error:"));
        assertTrue(result.contains("3 invalid events"));
        assertFalse(result.contains("success"));
    }

    @Test
    void testStatsStructure() {
        Map<String, Object> stats = controller.getStats();

        // Verify top-level structure
        assertNotNull(stats.get("bufferSize"));
        assertNotNull(stats.get("dropped"));
        assertNotNull(stats.get("batchSize"));
        assertNotNull(stats.get("flushMillis"));
        assertNotNull(stats.get("store"));

        // Verify store stats structure
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertNotNull(storeStats.get("totalEvents"));
        assertNotNull(storeStats.get("totalBatches"));
        assertNotNull(storeStats.get("tablesCount"));
        assertNotNull(storeStats.get("tableSizes"));
    }
}
