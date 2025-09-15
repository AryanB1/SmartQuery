package smartquery.index;

import smartquery.index.IndexTypes.*;
import smartquery.storage.ColumnStore;
import smartquery.ingest.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the index system.
 * Tests functionality, performance, and thread safety of indexes and IndexManager.
 */
public class IndexTests {
    
    private List<ColumnStore.Row> testRows;
    private IndexManager indexManager;
    
    @BeforeEach
    void setUp() {
        indexManager = new IndexManager();
        testRows = createTestData();
    }
    
    /**
     * Create test data with 1000 rows across different dimensions.
     */
    private List<ColumnStore.Row> createTestData() {
        List<ColumnStore.Row> rows = new ArrayList<>();
        String[] userIds = {"u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8", "u9", "u10"};
        String[] events = {"click", "purchase"};
        String[] regions = {"us", "eu", "apac"};
        
        Random random = new Random(42); // Fixed seed for reproducible tests
        
        for (int i = 0; i < 1000; i++) {
            Event event = new Event();
            event.ts = 1000000 + i;
            event.table = "events";
            event.userId = userIds[i % userIds.length];
            event.event = events[i % events.length];
            event.addProperty("region", regions[i % regions.length]);
            event.addProperty("price", String.valueOf(1 + random.nextInt(100)));
            
            rows.add(new ColumnStore.Row(event));
        }
        
        return rows;
    }
    
    @Test
    void testBitmapIndexEquals() {
        BitmapIndex index = new BitmapIndex("events", "props.region", "segment1");
        index.build(testRows);
        
        // Test equals lookup
        IntSet usMatches = index.lookupEquals("us");
        assertFalse(usMatches.isEmpty());
        
        // Count expected matches
        long expectedUs = testRows.stream()
            .mapToLong(row -> "us".equals(row.getProperty("region")) ? 1 : 0)
            .sum();
        
        assertEquals(expectedUs, usMatches.size());
        
        // Verify actual row IDs
        for (int rowId : usMatches) {
            assertTrue(rowId >= 0 && rowId < testRows.size());
            assertEquals("us", testRows.get(rowId).getProperty("region"));
        }
    }
    
    @Test
    void testBitmapIndexIn() {
        BitmapIndex index = new BitmapIndex("events", "props.region", "segment1");
        index.build(testRows);
        
        // Test IN lookup
        IntSet matches = index.lookupIn(Arrays.asList("us", "eu"));
        assertFalse(matches.isEmpty());
        
        // Count expected matches
        long expected = testRows.stream()
            .mapToLong(row -> {
                String region = row.getProperty("region");
                return ("us".equals(region) || "eu".equals(region)) ? 1 : 0;
            })
            .sum();
        
        assertEquals(expected, matches.size());
    }
    
    @Test
    void testBitmapIndexRangeUnsupported() {
        BitmapIndex index = new BitmapIndex("events", "props.region", "segment1");
        index.build(testRows);
        
        // Range queries should return null for bitmap indexes
        IntSet result = index.lookupRange(1.0, true, 10.0, true);
        assertNull(result);
    }
    
    @Test
    void testBTreeIndexRange() {
        BTreeIndex index = new BTreeIndex("events", "props.price", "segment1");
        index.build(testRows);
        
        // Test range lookup
        IntSet matches = index.lookupRange(10.0, true, 20.0, true);
        assertFalse(matches.isEmpty());
        
        // Verify all matches are in range
        for (int rowId : matches) {
            double price = Double.parseDouble(testRows.get(rowId).getProperty("price"));
            assertTrue(price >= 10.0 && price <= 20.0);
        }
    }
    
    @Test
    void testBTreeIndexEquals() {
        BTreeIndex index = new BTreeIndex("events", "props.price", "segment1");
        index.build(testRows);
        
        // Find a price that exists in test data
        String targetPrice = testRows.get(0).getProperty("price");
        
        IntSet matches = index.lookupEquals(targetPrice);
        assertFalse(matches.isEmpty());
        
        // Verify all matches have the target price
        for (int rowId : matches) {
            assertEquals(targetPrice, testRows.get(rowId).getProperty("price"));
        }
    }
    
    @Test
    void testIndexManagerLookup() {
        indexManager.registerSegment("events", "segment1", testRows.size());
        indexManager.ensureIndex("events", "props.region");
        
        // Simulate segment flush
        indexManager.onSegmentFlushed("events", "segment1", testRows);
        
        // Wait for background building (in real implementation, would use proper synchronization)
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Test lookup
        IndexManager.IndexQuery query = IndexManager.IndexQuery.equals("us");
        IndexManager.IndexLookupResult result = indexManager.lookup("events", "props.region", query);
        
        assertNotNull(result);
        assertNotNull(result.matches);
    }
    
    @Test
    void testAdaptiveIndexMLObservation() {
        AdaptiveIndexML ml = new AdaptiveIndexML();
        
        // Observe many queries on a column
        for (int i = 0; i < 100; i++) {
            ml.observeQuery("events", "props.region", false, 0.1); // High selectivity
        }
        
        // Get recommendations
        List<String> recommendations = ml.recommendToBuild("events", 1024 * 1024, 5);
        assertTrue(recommendations.contains("props.region"));
    }
    
    @Test
    void testAdaptiveIndexMLBudget() {
        AdaptiveIndexML ml = new AdaptiveIndexML();
        
        // Observe queries on multiple columns
        for (int i = 0; i < 50; i++) {
            ml.observeQuery("events", "props.region", false, 0.1);
            ml.observeQuery("events", "props.price", true, 0.2);
            ml.observeQuery("events", "userId", false, 0.05);
        }
        
        // Small budget should limit recommendations
        List<String> smallBudget = ml.recommendToBuild("events", 1000, 1);
        assertTrue(smallBudget.size() <= 1);
        
        // Large budget should allow more
        List<String> largeBudget = ml.recommendToBuild("events", 1024 * 1024, 10);
        assertTrue(largeBudget.size() > smallBudget.size());
    }
    
    @Test
    void testAdaptiveIndexMLDropStale() {
        AdaptiveIndexML ml = new AdaptiveIndexML();
        
        // Observe old queries
        ml.observeQuery("events", "stale_column", false, 0.1);
        
        // Wait and check for stale recommendations
        List<String> toDrop = ml.recommendToDrop("events", 5, 1000); // 1 second stale
        
        try {
            Thread.sleep(1100); // Wait for staleness
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        toDrop = ml.recommendToDrop("events", 5, 1000);
        assertTrue(toDrop.contains("stale_column"));
    }
    
    @Test
    @Timeout(10)
    void testIndexBackgroundTasks() throws Exception {
        IndexBackgroundTasks tasks = new IndexBackgroundTasks(2);
        
        BitmapIndex index = new BitmapIndex("events", "userId", "segment1");
        
        // Submit build task
        Future<Void> future = tasks.submitBuild(index, testRows);
        
        // Wait for completion
        future.get(5, TimeUnit.SECONDS);
        
        // Verify index was built
        assertFalse(index.lookupEquals("u1").isEmpty());
        
        tasks.shutdown();
    }
    
    @Test
    void testIndexCatalog() {
        IndexCatalog catalog = new IndexCatalog();
        
        // Test desired marking
        assertFalse(catalog.isDesired("events", "userId"));
        
        catalog.markDesired("events", "userId");
        assertTrue(catalog.isDesired("events", "userId"));
        
        // Test hit recording
        catalog.recordHit("events", "userId");
        IndexCatalog.IndexSpec spec = catalog.getSpec("events", "userId");
        assertNotNull(spec);
        assertEquals(1, spec.hitCount);
        
        // Test unmark
        catalog.unmarkDesired("events", "userId");
        assertFalse(catalog.isDesired("events", "userId"));
    }
    
    @Test
    void testIntArraySet() {
        IntArraySet set = new IntArraySet();
        
        // Test basic operations
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
        
        assertTrue(set.add(1));
        assertTrue(set.add(5));
        assertTrue(set.add(10));
        assertFalse(set.add(1)); // Duplicate
        
        assertEquals(3, set.size());
        assertFalse(set.isEmpty());
        
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertTrue(set.contains(10));
        assertFalse(set.contains(7));
        
        // Test iteration
        Set<Integer> iterated = new HashSet<>();
        for (int value : set) {
            iterated.add(value);
        }
        assertEquals(Set.of(1, 5, 10), iterated);
    }
    
    @Test
    void testBitSetIntSet() {
        BitSet bitSet = new BitSet();
        bitSet.set(1);
        bitSet.set(5);
        bitSet.set(10);
        
        BitSetIntSet set = new BitSetIntSet(bitSet);
        
        assertEquals(3, set.size());
        assertTrue(set.contains(1));
        assertTrue(set.contains(5));
        assertTrue(set.contains(10));
        assertFalse(set.contains(7));
        
        // Test iteration
        Set<Integer> iterated = new HashSet<>();
        for (int value : set) {
            iterated.add(value);
        }
        assertEquals(Set.of(1, 5, 10), iterated);
    }
    
    @Test
    @Timeout(30)
    void testThreadSafety() throws Exception {
        BitmapIndex index = new BitmapIndex("events", "userId", "segment1");
        index.build(testRows);
        
        // Run concurrent lookups
        int numThreads = 10;
        int lookupsPerThread = 100;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Integer>> futures = new ArrayList<>();
        
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                int totalResults = 0;
                for (int j = 0; j < lookupsPerThread; j++) {
                    IntSet results = index.lookupEquals("u" + (j % 10 + 1));
                    totalResults += results.size();
                }
                return totalResults;
            }));
        }
        
        // Collect results
        int totalResults = 0;
        for (Future<Integer> future : futures) {
            totalResults += future.get();
        }
        
        assertTrue(totalResults > 0);
        executor.shutdown();
    }
    
    @Test
    void testMemoryEstimation() {
        BitmapIndex index = new BitmapIndex("events", "props.region", "segment1");
        index.build(testRows);
        
        long memoryBytes = index.memoryBytes();
        assertTrue(memoryBytes > 0);
        
        // Memory should be reasonable (less than 1MB for this test data)
        assertTrue(memoryBytes < 1024 * 1024);
    }
    
    @Test
    void testIndexStats() {
        BitmapIndex index = new BitmapIndex("events", "props.region", "segment1");
        index.build(testRows);
        
        Map<String, Object> stats = index.stats();
        assertNotNull(stats);
        
        assertEquals("bitmap", stats.get("type"));
        assertEquals("events", stats.get("table"));
        assertEquals("props.region", stats.get("column"));
        assertEquals("segment1", stats.get("segmentId"));
        assertTrue((Integer) stats.get("distinctValues") > 0);
        assertTrue((Integer) stats.get("rowCount") > 0);
        assertTrue((Long) stats.get("memoryBytes") > 0);
    }
}