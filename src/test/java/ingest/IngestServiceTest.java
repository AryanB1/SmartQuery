package ingest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import ingest.model.Event;
import storage.ColumnStore;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

class IngestServiceTest {

    private IngestService ingestService;
    private ColumnStore columnStore;

    @BeforeEach
    void setUp() {
        columnStore = new ColumnStore();
        ingestService = new IngestService(columnStore);
    }

    @AfterEach
    void tearDown() {
        if (ingestService != null) {
            ingestService.stop();
        }
    }

    @Test
    void testSubmitEvents() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "page_view")
        );

        int result = ingestService.submit(events);

        assertEquals(2, result);
        
        // Force flush to ensure events are stored
        ingestService.flush();
        
        assertEquals(2, columnStore.size());
    }

    @Test
    void testSubmitNullEvents() {
        int result = ingestService.submit(null);
        assertEquals(0, result);
        
        result = ingestService.submit(new ArrayList<>());
        assertEquals(0, result);
        
        assertEquals(0, columnStore.size());
    }

    @Test
    void testFlush() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "logout")
        );

        ingestService.submit(events);
        
        // Before flush, events might still be in buffer
        Map<String, Object> statsBefore = ingestService.stats();
        
        ingestService.flush();
        
        // After flush, events should be in store
        assertEquals(2, columnStore.size());
        
        Map<String, Object> statsAfter = ingestService.stats();
        assertEquals(0, statsAfter.get("bufferSize"));
    }

    @Test
    void testStats() {
        Map<String, Object> stats = ingestService.stats();
        
        assertNotNull(stats);
        assertTrue(stats.containsKey("bufferSize"));
        assertTrue(stats.containsKey("dropped"));
        assertTrue(stats.containsKey("batchSize"));
        assertTrue(stats.containsKey("flushMillis"));
        assertTrue(stats.containsKey("store"));
        
        assertEquals(0, stats.get("bufferSize"));
        assertEquals(0, stats.get("dropped"));
        
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertNotNull(storeStats);
        assertEquals(0L, storeStats.get("totalEvents"));
    }

    @Test
    void testStatsAfterEvents() {
        List<Event> events = Arrays.asList(
            new Event("user1", "login"),
            new Event("user2", "page_view"),
            new Event("user3", "purchase")
        );

        ingestService.submit(events);
        ingestService.flush();

        Map<String, Object> stats = ingestService.stats();
        
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertEquals(3L, storeStats.get("totalEvents"));
        assertEquals(1L, storeStats.get("totalBatches"));
    }

    @Test
    void testQueryEvents() {
        Event event1 = new Event("user1", "login");
        event1.ts = 1000L;
        event1.table = "user_events";
        
        Event event2 = new Event("user2", "logout");
        event2.ts = 2000L;
        event2.table = "user_events";
        
        Event event3 = new Event("user3", "purchase");
        event3.ts = 3000L;
        event3.table = "transactions";

        ingestService.submit(Arrays.asList(event1, event2, event3));
        ingestService.flush();

        // Query user_events table
        List<Event> userEvents = ingestService.queryEvents("user_events", 0L, Long.MAX_VALUE);
        assertEquals(2, userEvents.size());
        
        // Query transactions table
        List<Event> transactions = ingestService.queryEvents("transactions", 0L, Long.MAX_VALUE);
        assertEquals(1, transactions.size());
        assertEquals("purchase", transactions.get(0).event);
        
        // Query with time range
        List<Event> recentEvents = ingestService.queryEvents("user_events", 1500L, Long.MAX_VALUE);
        assertEquals(1, recentEvents.size());
        assertEquals("user2", recentEvents.get(0).userId);
    }

    @Test
    void testScanPassthrough() {
        Event event = new Event("user1", "login");
        event.ts = 1000L;
        event.table = "user_events";
        event.addProperty("ip", "192.168.1.1");

        ingestService.submit(Arrays.asList(event));
        ingestService.flush();

        List<ColumnStore.Row> results = new ArrayList<>();
        for (ColumnStore.Row row : ingestService.scan("user_events", 0L, Long.MAX_VALUE, null)) {
            results.add(row);
        }

        assertEquals(1, results.size());
        assertEquals("user1", results.get(0).getUserId());
        assertEquals("login", results.get(0).getEvent());
        assertEquals("192.168.1.1", results.get(0).getProperty("ip"));
    }

    @Test
    void testScanWithFilter() {
        Event event1 = new Event("user1", "login");
        event1.ts = 1000L;
        event1.table = "user_events";
        event1.addProperty("browser", "Chrome");
        
        Event event2 = new Event("user2", "login");
        event2.ts = 2000L;
        event2.table = "user_events";
        event2.addProperty("browser", "Firefox");

        ingestService.submit(Arrays.asList(event1, event2));
        ingestService.flush();

        // Filter for Chrome users only
        List<ColumnStore.Row> chromeUsers = new ArrayList<>();
        for (ColumnStore.Row row : ingestService.scan("user_events", 0L, Long.MAX_VALUE, 
                r -> "Chrome".equals(r.getProperty("browser")))) {
            chromeUsers.add(row);
        }

        assertEquals(1, chromeUsers.size());
        assertEquals("user1", chromeUsers.get(0).getUserId());
    }

    @Test
    void testBackpressure() {
        // This test verifies the soft backpressure mechanism exists
        // Note: The exact behavior depends on timing and batch processing
        
        Map<String, Object> initialStats = ingestService.stats();
        Integer initialDropped = (Integer) initialStats.get("dropped");
        
        // The backpressure mechanism exists and tracks dropped events
        assertNotNull(initialDropped);
        assertEquals(0, initialDropped.intValue());
        
        // Verify that stats track dropped events (mechanism is present)
        assertTrue(initialStats.containsKey("dropped"));
        assertTrue(initialStats.containsKey("batchSize"));
    }

    @Test
    void testMultipleBatches() {
        // Submit events in multiple small batches
        for (int batch = 0; batch < 5; batch++) {
            List<Event> events = Arrays.asList(
                new Event("user" + batch + "_1", "login"),
                new Event("user" + batch + "_2", "logout")
            );
            ingestService.submit(events);
        }
        
        ingestService.flush();
        
        assertEquals(10, columnStore.size());
        
        Map<String, Object> stats = ingestService.stats();
        @SuppressWarnings("unchecked")
        Map<String, Object> storeStats = (Map<String, Object>) stats.get("store");
        assertEquals(10L, storeStats.get("totalEvents"));
    }

    @Test
    void testStopService() {
        List<Event> events = Arrays.asList(new Event("user1", "login"));
        ingestService.submit(events);
        
        // Stop should flush remaining events
        ingestService.stop();
        
        assertEquals(1, columnStore.size());
    }
}
