package integration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import smartquery.SmartQueryApplication;
import ingest.model.Event;
import ingest.http.HttpIngestController;
import storage.ColumnStore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Integration tests that verify the complete SmartQuery system works end-to-end.
 */
class SmartQueryIntegrationTest {

    private SmartQueryApplication app;
    private HttpIngestController controller;

    @BeforeEach
    void setUp() {
        app = new SmartQueryApplication();
        controller = app.getHttpController();
        
        // Give the application a moment to initialize
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterEach
    void tearDown() {
        if (app != null) {
            app.getIngestService().stop();
        }
    }

    @Test
    void testEndToEndEventProcessing() {
        // Create a variety of events
        List<Event> events = Arrays.asList(
            new Event("user1", "login").addProperty("ip", "192.168.1.1").addProperty("browser", "Chrome"),
            new Event("user1", "page_view").addProperty("page", "/dashboard").addProperty("duration", "45"),
            new Event("user2", "login").addProperty("ip", "192.168.1.2").addProperty("browser", "Firefox"),
            new Event("user1", "purchase").addProperty("amount", "99.99").addProperty("currency", "USD"),
            new Event("user2", "logout").addProperty("session_duration", "1800")
        );

        // Set different tables for some events
        events.get(3).table = "transactions"; // purchase event
        
        // Ingest the events
        String result = controller.ingestEvents(events);
        assertTrue(result.contains("success"));
        assertTrue(result.contains("accepted 5 events"));

        // Force flush to ensure all events are stored
        app.getIngestService().flush();

        // Verify events are stored
        ColumnStore store = app.getColumnStore();
        assertEquals(5, store.size());
        assertEquals(2, store.getTableNames().size()); // "events" and "transactions"
        assertTrue(store.getTableNames().contains("events"));
        assertTrue(store.getTableNames().contains("transactions"));

        // Verify we can query the data
        List<Event> userEvents = app.getIngestService().queryEvents("events", 0L, Long.MAX_VALUE);
        assertEquals(4, userEvents.size()); // 4 events in "events" table

        List<Event> transactions = app.getIngestService().queryEvents("transactions", 0L, Long.MAX_VALUE);
        assertEquals(1, transactions.size()); // 1 event in "transactions" table
        assertEquals("purchase", transactions.get(0).event);
        assertEquals("99.99", transactions.get(0).getProperty("amount"));

        // Verify system health
        String health = controller.getHealth();
        assertEquals("healthy", health);

        // Verify stats
        Map<String, Object> stats = controller.getStats();
        assertNotNull(stats);
        
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertEquals(5L, storeStats.get("totalEvents"));
        assertEquals(2, storeStats.get("tablesCount"));
    }

    @Test
    void testMultiUserScenario() {
        // Simulate a multi-user scenario with different event types
        List<Event> batch1 = Arrays.asList(
            new Event("alice", "register").addProperty("email", "alice@example.com"),
            new Event("bob", "register").addProperty("email", "bob@example.com"),
            new Event("charlie", "register").addProperty("email", "charlie@example.com")
        );

        List<Event> batch2 = Arrays.asList(
            new Event("alice", "login").addProperty("ip", "192.168.1.10"),
            new Event("bob", "login").addProperty("ip", "192.168.1.11"),
            new Event("alice", "page_view").addProperty("page", "/profile"),
            new Event("bob", "page_view").addProperty("page", "/settings")
        );

        List<Event> batch3 = Arrays.asList(
            new Event("alice", "purchase").addProperty("item", "book").addProperty("price", "19.99"),
            new Event("charlie", "login").addProperty("ip", "192.168.1.12"),
            new Event("bob", "logout")
        );

        // Set transactions table for purchases
        batch3.get(0).table = "purchases";

        // Ingest batches sequentially
        String result1 = controller.ingestEvents(batch1);
        String result2 = controller.ingestEvents(batch2);
        String result3 = controller.ingestEvents(batch3);

        assertTrue(result1.contains("success"));
        assertTrue(result2.contains("success"));
        assertTrue(result3.contains("success"));

        // Force flush
        app.getIngestService().flush();

        // Verify total events
        assertEquals(10, app.getColumnStore().size());
        assertEquals(2, app.getColumnStore().getTableNames().size());

        // Query specific user activity
        List<Event> aliceEvents = app.getIngestService().queryEvents("events", 0L, Long.MAX_VALUE)
            .stream()
            .filter(e -> "alice".equals(e.userId))
            .collect(java.util.stream.Collectors.toList());
        assertEquals(3, aliceEvents.size()); // register, login, page_view in events table (purchase is in purchases table)

        List<Event> purchases = app.getIngestService().queryEvents("purchases", 0L, Long.MAX_VALUE);
        assertEquals(1, purchases.size());
        assertEquals("alice", purchases.get(0).userId);

        // Verify system remains healthy
        assertEquals("healthy", controller.getHealth());
    }

    @Test
    void testErrorHandlingIntegration() {
        // Test various error scenarios
        
        // 1. Empty event list
        String result1 = controller.ingestEvents(null);
        assertEquals("error: empty event list", result1);

        // 2. Invalid events
        List<Event> invalidEvents = Arrays.asList(
            new Event("user1", "valid_event"),
            new Event("user2", null), // invalid
            new Event("user3", "")    // invalid
        );
        String result2 = controller.ingestEvents(invalidEvents);
        assertTrue(result2.contains("error:"));
        assertTrue(result2.contains("2 invalid events"));

        // 3. System should still be healthy after errors
        assertEquals("healthy", controller.getHealth());

        // 4. Valid events should still work after errors
        List<Event> validEvents = Arrays.asList(
            new Event("user4", "recovery_event")
        );
        String result3 = controller.ingestEvents(validEvents);
        assertTrue(result3.contains("success"));

        app.getIngestService().flush();
        assertEquals(1, app.getColumnStore().size());
    }

    @Test
    void testTimeBasedQuerying() {
        long startTime = System.currentTimeMillis();
        
        Event event1 = new Event("user1", "early_event");
        event1.ts = startTime;
        
        Event event2 = new Event("user1", "middle_event");  
        event2.ts = startTime + 1000; // 1 second later
        
        Event event3 = new Event("user1", "late_event");
        event3.ts = startTime + 2000; // 2 seconds later

        List<Event> events = Arrays.asList(event1, event2, event3);
        controller.ingestEvents(events);
        app.getIngestService().flush();

        // Query different time ranges
        List<Event> allEvents = app.getIngestService().queryEvents("events", 0L, Long.MAX_VALUE);
        assertEquals(3, allEvents.size());

        List<Event> earlyEvents = app.getIngestService().queryEvents("events", startTime, startTime + 500);
        assertEquals(1, earlyEvents.size());
        assertEquals("early_event", earlyEvents.get(0).event);

        List<Event> laterEvents = app.getIngestService().queryEvents("events", startTime + 1500, Long.MAX_VALUE);
        assertEquals(1, laterEvents.size());
        assertEquals("late_event", laterEvents.get(0).event);
    }
}
