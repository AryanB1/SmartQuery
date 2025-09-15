package smartquery.index;

import smartquery.index.IndexTypes.*;
import smartquery.storage.ColumnStore;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Index manager responsible for index lifecycle, lookups, and adaptive management.
 * Provides the main entry point for secondary indexing in SmartQuery.
 */
public class IndexManager {
    
    // Configuration from system properties
    private final long memoryBudgetMb = Long.getLong("smartquery.index.memoryBudgetMb", 256L);
    private final int maxNewPerTick = Integer.getInteger("smartquery.index.maxNewPerTick", 2);
    private final long staleDropMs = Long.getLong("smartquery.index.staleDropMs", 604_800_000L); // 7 days
    private final int adaptiveTickSeconds = Integer.getInteger("smartquery.index.adaptiveTickSeconds", 60);
    
    // Core components
    private final IndexCatalog catalog = new IndexCatalog();
    private final AdaptiveIndexML adaptiveML = new AdaptiveIndexML();
    private final IndexBackgroundTasks backgroundTasks = new IndexBackgroundTasks();
    
    // Index storage: table -> column -> segmentId -> index
    private final Map<String, Map<String, Map<String, SecondaryIndex>>> indexes = new ConcurrentHashMap<>();
    
    // Segment metadata: table -> segmentId -> segment info
    private final Map<String, Map<String, SegmentInfo>> segments = new ConcurrentHashMap<>();
    
    // Thread safety
    private final ReadWriteLock indexLock = new ReentrantReadWriteLock();
    
    // Adaptive indexing scheduler
    private final ScheduledExecutorService adaptiveScheduler;
    private volatile boolean shutdown = false;
    
    /**
     * Create index manager and start adaptive indexing.
     */
    public IndexManager() {
        this.adaptiveScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "adaptive-indexer");
            t.setDaemon(true);
            return t;
        });
        
        // Start adaptive tick
        adaptiveScheduler.scheduleAtFixedRate(
            this::runAdaptiveTick, 
            adaptiveTickSeconds, 
            adaptiveTickSeconds, 
            TimeUnit.SECONDS
        );
    }
    
    /**
     * Register a new segment.
     */
    public void registerSegment(String table, String segmentId, int rowCount) {
        segments.computeIfAbsent(table, k -> new ConcurrentHashMap<>())
                .put(segmentId, new SegmentInfo(segmentId, rowCount, System.currentTimeMillis()));
    }
    
    /**
     * Unregister a segment and clean up its indexes.
     */
    public void unregisterSegment(String table, String segmentId) {
        // Remove segment info
        Map<String, SegmentInfo> tableSegments = segments.get(table);
        if (tableSegments != null) {
            tableSegments.remove(segmentId);
        }
        
        // Remove all indexes for this segment
        indexLock.writeLock().lock();
        try {
            Map<String, Map<String, SecondaryIndex>> tableIndexes = indexes.get(table);
            if (tableIndexes != null) {
                for (Map<String, SecondaryIndex> columnIndexes : tableIndexes.values()) {
                    columnIndexes.remove(segmentId);
                }
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    
    /**
     * Called when a segment is flushed - build indexes for desired columns.
     */
    public void onSegmentFlushed(String table, String segmentId, List<ColumnStore.Row> rows) {
        if (rows.isEmpty()) {
            return;
        }
        
        // Find columns that should have indexes
        List<String> desiredColumns = new ArrayList<>();
        for (IndexCatalog.IndexSpec spec : catalog.listAll()) {
            if (spec.table.equals(table)) {
                desiredColumns.add(spec.column);
            }
        }
        
        // Build indexes for desired columns
        for (String column : desiredColumns) {
            SecondaryIndex index = createIndex(table, column, segmentId);
            
            // Build in background
            backgroundTasks.submitBuild(index, rows).thenRun(() -> {
                // Store the built index
                indexLock.writeLock().lock();
                try {
                    indexes.computeIfAbsent(table, k -> new ConcurrentHashMap<>())
                           .computeIfAbsent(column, k -> new ConcurrentHashMap<>())
                           .put(segmentId, index);
                } finally {
                    indexLock.writeLock().unlock();
                }
            });
        }
    }
    
    /**
     * Ensure an index exists for the given table and column.
     * Returns true if index is available after the call.
     */
    public boolean ensureIndex(String table, String column) {
        // Mark as desired in catalog
        catalog.markDesired(table, column);
        
        // Check if we already have indexes for existing segments
        indexLock.readLock().lock();
        try {
            Map<String, Map<String, SecondaryIndex>> tableIndexes = indexes.get(table);
            if (tableIndexes != null && tableIndexes.containsKey(column)) {
                return !tableIndexes.get(column).isEmpty();
            }
        } finally {
            indexLock.readLock().unlock();
        }
        
        // Build indexes for existing segments
        Map<String, SegmentInfo> tableSegments = segments.get(table);
        if (tableSegments != null) {
            for (String segmentId : tableSegments.keySet()) {
                // Note: In a real implementation, we'd need access to segment row data
                // For now, we just mark the index as desired
            }
        }
        
        return false; // Will be available after background building completes
    }
    
    /**
     * Drop an index for the given table and column.
     */
    public void dropIndex(String table, String column) {
        catalog.unmarkDesired(table, column);
        
        indexLock.writeLock().lock();
        try {
            Map<String, Map<String, SecondaryIndex>> tableIndexes = indexes.get(table);
            if (tableIndexes != null) {
                tableIndexes.remove(column);
            }
        } finally {
            indexLock.writeLock().unlock();
        }
    }
    
    /**
     * Lookup rows matching the query across all segments.
     */
    public IndexLookupResult lookup(String table, String column, IndexQuery query) {
        Map<String, IntSet> matches = new HashMap<>();
        boolean exact = true;
        long rowsConsidered = 0;
        
        indexLock.readLock().lock();
        try {
            Map<String, Map<String, SecondaryIndex>> tableIndexes = indexes.get(table);
            if (tableIndexes == null || !tableIndexes.containsKey(column)) {
                return new IndexLookupResult(Collections.emptyMap(), false, 0);
            }
            
            Map<String, SecondaryIndex> columnIndexes = tableIndexes.get(column);
            
            for (Map.Entry<String, SecondaryIndex> entry : columnIndexes.entrySet()) {
                String segmentId = entry.getKey();
                SecondaryIndex index = entry.getValue();
                
                IntSet segmentMatches = executeQuery(index, query);
                if (segmentMatches != null && !segmentMatches.isEmpty()) {
                    matches.put(segmentId, segmentMatches);
                    rowsConsidered += segmentMatches.size();
                }
            }
            
        } finally {
            indexLock.readLock().unlock();
        }
        
        // Record usage for adaptive indexing
        catalog.recordHit(table, column);
        
        return new IndexLookupResult(matches, exact, rowsConsidered);
    }
    
    /**
     * Record that a column was referenced in a query.
     */
    public void recordQueryUsage(String table, String column) {
        // Note: In a real implementation, we'd need more context about the query
        // For now, we'll assume it's an equals query with moderate selectivity
        adaptiveML.observeQuery(table, column, false, 0.1);
    }
    
    /**
     * Run adaptive indexing tick - evaluate and apply recommendations.
     */
    public void runAdaptiveTick() {
        if (shutdown) {
            return;
        }
        
        try {
            // Get unique tables
            Set<String> tables = new HashSet<>();
            tables.addAll(segments.keySet());
            tables.addAll(indexes.keySet());
            
            for (String table : tables) {
                runAdaptiveTickForTable(table);
            }
            
        } catch (Exception e) {
            // Log error but continue (in real implementation, use proper logging)
            System.err.println("Error in adaptive tick: " + e.getMessage());
        }
    }
    
    private void runAdaptiveTickForTable(String table) {
        long memoryBudgetBytes = memoryBudgetMb * 1024 * 1024;
        
        // Get recommendations
        List<String> toBuild = adaptiveML.recommendToBuild(table, memoryBudgetBytes, maxNewPerTick);
        List<String> toDrop = adaptiveML.recommendToDrop(table, maxNewPerTick, staleDropMs);
        
        // Apply recommendations
        for (String column : toBuild) {
            ensureIndex(table, column);
        }
        
        for (String column : toDrop) {
            dropIndex(table, column);
        }
    }
    
    /**
     * Get comprehensive statistics.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        
        // Index counts
        int totalIndexes = 0;
        long totalMemory = 0;
        
        indexLock.readLock().lock();
        try {
            for (Map<String, Map<String, SecondaryIndex>> tableIndexes : indexes.values()) {
                for (Map<String, SecondaryIndex> columnIndexes : tableIndexes.values()) {
                    totalIndexes += columnIndexes.size();
                    for (SecondaryIndex index : columnIndexes.values()) {
                        totalMemory += index.memoryBytes();
                    }
                }
            }
        } finally {
            indexLock.readLock().unlock();
        }
        
        stats.put("totalIndexes", totalIndexes);
        stats.put("memoryBytes", totalMemory);
        stats.put("memoryMB", totalMemory / (1024 * 1024));
        stats.put("memoryBudgetMB", memoryBudgetMb);
        
        // Segment counts
        int totalSegments = segments.values().stream()
            .mapToInt(Map::size)
            .sum();
        stats.put("totalSegments", totalSegments);
        
        // Background task stats
        stats.put("backgroundTasks", backgroundTasks.stats());
        
        // Catalog stats
        stats.put("catalog", catalog.stats());
        
        // Adaptive ML stats
        stats.put("adaptiveML", adaptiveML.stats());
        
        return stats;
    }
    
    /**
     * Shutdown the index manager.
     */
    public void shutdown() {
        shutdown = true;
        adaptiveScheduler.shutdown();
        backgroundTasks.shutdown();
    }
    
    private SecondaryIndex createIndex(String table, String column, String segmentId) {
        // Choose index type based on column
        if (column.equals("ts") || looksNumeric(column)) {
            return new BTreeIndex(table, column, segmentId);
        } else {
            return new BitmapIndex(table, column, segmentId);
        }
    }
    
    private boolean looksNumeric(String column) {
        // Simple heuristic - property columns might be numeric
        return column.startsWith("props.") && 
               (column.contains("price") || column.contains("amount") || 
                column.contains("count") || column.contains("size"));
    }
    
    private IntSet executeQuery(SecondaryIndex index, IndexQuery query) {
        try {
            switch (query.type) {
                case EQUALS:
                    if (query.inValues != null && !query.inValues.isEmpty()) {
                        return index.lookupEquals(query.inValues.get(0));
                    }
                    break;
                    
                case IN:
                    if (query.inValues != null) {
                        return index.lookupIn(query.inValues);
                    }
                    break;
                    
                case RANGE:
                    return index.lookupRange(query.rangeLo, query.includeLo, 
                                           query.rangeHi, query.includeHi);
            }
        } catch (Exception e) {
            // Log error and return empty result
            System.err.println("Error executing index query: " + e.getMessage());
        }
        
        return IntSet.empty();
    }
    
    /**
     * Index query specification.
     */
    public static class IndexQuery {
        public enum Type { EQUALS, IN, RANGE }
        
        public final Type type;
        public final List<String> inValues;
        public final Double rangeLo;
        public final Double rangeHi;
        public final boolean includeLo;
        public final boolean includeHi;
        
        // For equals/in queries
        public IndexQuery(Type type, List<String> inValues) {
            this.type = type;
            this.inValues = inValues;
            this.rangeLo = null;
            this.rangeHi = null;
            this.includeLo = false;
            this.includeHi = false;
        }
        
        // For range queries
        public IndexQuery(Double rangeLo, boolean includeLo, Double rangeHi, boolean includeHi) {
            this.type = Type.RANGE;
            this.inValues = null;
            this.rangeLo = rangeLo;
            this.rangeHi = rangeHi;
            this.includeLo = includeLo;
            this.includeHi = includeHi;
        }
        
        public static IndexQuery equals(String value) {
            return new IndexQuery(Type.EQUALS, Collections.singletonList(value));
        }
        
        public static IndexQuery in(List<String> values) {
            return new IndexQuery(Type.IN, values);
        }
        
        public static IndexQuery range(Double lo, boolean includeLo, Double hi, boolean includeHi) {
            return new IndexQuery(lo, includeLo, hi, includeHi);
        }
    }
    
    /**
     * Result of an index lookup.
     */
    public static class IndexLookupResult {
        public final Map<String, IntSet> matches; // segmentId -> row IDs
        public final boolean exact;               // true if result is exact
        public final long rowsConsidered;         // diagnostic info
        
        public IndexLookupResult(Map<String, IntSet> matches, boolean exact, long rowsConsidered) {
            this.matches = matches;
            this.exact = exact;
            this.rowsConsidered = rowsConsidered;
        }
    }
    
    /**
     * Segment metadata.
     */
    private static class SegmentInfo {
        final String segmentId;
        final int rowCount;
        final long createdAt;
        
        SegmentInfo(String segmentId, int rowCount, long createdAt) {
            this.segmentId = segmentId;
            this.rowCount = rowCount;
            this.createdAt = createdAt;
        }
    }
}