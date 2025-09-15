package smartquery.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import smartquery.ingest.model.Event;
import smartquery.storage.ColumnStore.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

class ColumnStoreTest {

    private ColumnStore columnStore;
    private Event testEvent1;
    private Event testEvent2;
    private Event testEvent3;

    @BeforeEach
    void setUp() {
        columnStore = new ColumnStore();
        
        testEvent1 = new Event("user123", "login")
            .addProperty("ip", "192.168.1.1")
            .addProperty("browser", "Chrome");
        testEvent1.ts = 1000L;
        testEvent1.table = "user_events";
        
        testEvent2 = new Event("user456", "page_view")
            .addProperty("page", "/dashboard")
            .addProperty("referrer", "google.com");
        testEvent2.ts = 2000L;
        testEvent2.table = "user_events";
        
        testEvent3 = new Event("user789", "purchase")
            .addProperty("amount", "29.99")
            .addProperty("currency", "USD");
        testEvent3.ts = 3000L;
        testEvent3.table = "transactions";
    }

    @Test
    void testAppendBatchSingleTable() {
        List<Event> events = Arrays.asList(testEvent1, testEvent2);
        
        columnStore.appendBatch(events);
        
        assertEquals(2, columnStore.size());
        assertEquals(1, columnStore.getTableNames().size());
        assertTrue(columnStore.getTableNames().contains("user_events"));
        
        Map<String, Object> stats = columnStore.stats();
        assertEquals(2L, stats.get("totalEvents"));
        assertEquals(1L, stats.get("totalBatches"));
    }

    @Test
    void testAppendBatchMultipleTables() {
        List<Event> events = Arrays.asList(testEvent1, testEvent2, testEvent3);
        
        columnStore.appendBatch(events);
        
        assertEquals(3, columnStore.size());
        assertEquals(2, columnStore.getTableNames().size());
        assertTrue(columnStore.getTableNames().contains("user_events"));
        assertTrue(columnStore.getTableNames().contains("transactions"));
        
        @SuppressWarnings("unchecked")
        Map<String, Integer> tableSizes = (Map<String, Integer>) columnStore.stats().get("tableSizes");
        assertEquals(2, tableSizes.get("user_events").intValue());
        assertEquals(1, tableSizes.get("transactions").intValue());
    }

    @Test
    void testAppendBatchWithNullAndEmpty() {
        columnStore.appendBatch(null);
        assertEquals(0, columnStore.size());
        
        columnStore.appendBatch(new ArrayList<>());
        assertEquals(0, columnStore.size());
        
        Map<String, Object> stats = columnStore.stats();
        assertEquals(0L, stats.get("totalEvents"));
        assertEquals(0L, stats.get("totalBatches"));
    }

    @Test
    void testAppendBatchWithNullTable() {
        Event eventWithNullTable = new Event("user123", "test");
        eventWithNullTable.table = null;
        
        columnStore.appendBatch(Arrays.asList(eventWithNullTable));
        
        assertEquals(1, columnStore.size());
        assertTrue(columnStore.getTableNames().contains("events")); // Default table
    }

    @Test
    void testScanWithTimeRange() {
        List<Event> events = Arrays.asList(testEvent1, testEvent2, testEvent3);
        columnStore.appendBatch(events);
        
        // Scan user_events table from 1000 to 2500
        List<Row> results = new ArrayList<>();
        for (Row row : columnStore.scan("user_events", 1000L, 2500L, null)) {
            results.add(row);
        }
        
        assertEquals(2, results.size());
        assertEquals("user123", results.get(0).getUserId());
        assertEquals("user456", results.get(1).getUserId());
    }

    @Test
    void testScanWithFilter() {
        List<Event> events = Arrays.asList(testEvent1, testEvent2, testEvent3);
        columnStore.appendBatch(events);
        
        // Scan with filter for Chrome browser
        List<Row> results = new ArrayList<>();
        for (Row row : columnStore.scan("user_events", 0L, Long.MAX_VALUE, 
                r -> "Chrome".equals(r.getProperty("browser")))) {
            results.add(row);
        }
        
        assertEquals(1, results.size());
        assertEquals("user123", results.get(0).getUserId());
    }

    @Test
    void testScanNonexistentTable() {
        columnStore.appendBatch(Arrays.asList(testEvent1));
        
        List<Row> results = new ArrayList<>();
        for (Row row : columnStore.scan("nonexistent", 0L, Long.MAX_VALUE, null)) {
            results.add(row);
        }
        
        assertTrue(results.isEmpty());
    }

    @Test
    void testStats() {
        List<Event> batch1 = Arrays.asList(testEvent1, testEvent2);
        List<Event> batch2 = Arrays.asList(testEvent3);
        
        columnStore.appendBatch(batch1);
        columnStore.appendBatch(batch2);
        
        Map<String, Object> stats = columnStore.stats();
        
        assertEquals(3L, stats.get("totalEvents"));
        assertEquals(2L, stats.get("totalBatches"));
        assertEquals(2, stats.get("tablesCount"));
        
        @SuppressWarnings("unchecked")
        Map<String, Integer> tableSizes = (Map<String, Integer>) stats.get("tableSizes");
        assertNotNull(tableSizes);
        assertEquals(2, tableSizes.get("user_events").intValue());
        assertEquals(1, tableSizes.get("transactions").intValue());
    }

    @Test
    void testClear() {
        columnStore.appendBatch(Arrays.asList(testEvent1, testEvent2, testEvent3));
        assertEquals(3, columnStore.size());
        
        columnStore.clear();
        
        assertEquals(0, columnStore.size());
        assertTrue(columnStore.getTableNames().isEmpty());
        
        Map<String, Object> stats = columnStore.stats();
        assertEquals(0L, stats.get("totalEvents"));
        assertEquals(0L, stats.get("totalBatches"));
        assertEquals(0, stats.get("tablesCount"));
    }

    @Test
    void testDeprecatedAddAll() {
        List<Event> events = Arrays.asList(testEvent1, testEvent2);
        
        // Test deprecated method still works
        @SuppressWarnings("deprecation")
        int sizeBefore = columnStore.size();
        columnStore.addAll(events);
        
        assertEquals(sizeBefore + 2, columnStore.size());
    }

    @Test
    void testRowWrapper() {
        Row row = new Row(testEvent1);
        
        assertEquals(testEvent1.ts, row.getTimestamp());
        assertEquals(testEvent1.table, row.getTable());
        assertEquals(testEvent1.userId, row.getUserId());
        assertEquals(testEvent1.event, row.getEvent());
        assertEquals(testEvent1.props, row.getProps());
        assertEquals("192.168.1.1", row.getProperty("ip"));
        assertEquals("Chrome", row.getProperty("browser"));
        assertNull(row.getProperty("nonexistent"));
        assertEquals(testEvent1, row.getOriginalEvent());
        
        String toString = row.toString();
        assertTrue(toString.contains("user123"));
        assertTrue(toString.contains("login"));
        assertTrue(toString.contains("user_events"));
    }

    @Test
    void testConcurrentAccess() throws InterruptedException {
        final int numThreads = 10;
        final int eventsPerThread = 100;
        
        Thread[] threads = new Thread[numThreads];
        
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                List<Event> events = new ArrayList<>();
                for (int j = 0; j < eventsPerThread; j++) {
                    Event event = new Event("user" + threadId + "_" + j, "test_event");
                    event.ts = System.currentTimeMillis() + j;
                    events.add(event);
                }
                columnStore.appendBatch(events);
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        assertEquals(numThreads * eventsPerThread, columnStore.size());
        
        Map<String, Object> stats = columnStore.stats();
        assertEquals((long) numThreads * eventsPerThread, stats.get("totalEvents"));
        assertEquals((long) numThreads, stats.get("totalBatches"));
    }
}