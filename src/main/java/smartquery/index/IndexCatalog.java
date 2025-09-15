package smartquery.index;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Index catalog that maintains metadata about desired indexes.
 * Tracks which indexes should be created and their usage statistics.
 */
public class IndexCatalog {
    
    private final Map<String, IndexSpec> indexSpecs = new ConcurrentHashMap<>();
    
    /**
     * Check if an index is desired for the given table and column.
     */
    public boolean isDesired(String table, String column) {
        return indexSpecs.containsKey(makeKey(table, column));
    }
    
    /**
     * Mark an index as desired for the given table and column.
     */
    public void markDesired(String table, String column) {
        String key = makeKey(table, column);
        indexSpecs.put(key, new IndexSpec(table, column, System.currentTimeMillis()));
    }
    
    /**
     * Remove the desired status for an index.
     */
    public void unmarkDesired(String table, String column) {
        indexSpecs.remove(makeKey(table, column));
    }
    
    /**
     * List all index specifications.
     */
    public List<IndexSpec> listAll() {
        return new ArrayList<>(indexSpecs.values());
    }
    
    /**
     * Record a hit (usage) for an index.
     */
    public void recordHit(String table, String column) {
        String key = makeKey(table, column);
        IndexSpec spec = indexSpecs.get(key);
        if (spec != null) {
            spec.hitCount++;
            spec.lastUsedAt = System.currentTimeMillis();
        }
    }
    
    /**
     * Get index spec for a table and column.
     */
    public IndexSpec getSpec(String table, String column) {
        return indexSpecs.get(makeKey(table, column));
    }
    
    /**
     * Update build cost for an index.
     */
    public void recordBuildCost(String table, String column, long buildTimeMs) {
        String key = makeKey(table, column);
        IndexSpec spec = indexSpecs.get(key);
        if (spec != null) {
            spec.buildCost = buildTimeMs;
        }
    }
    
    /**
     * Get indexes that haven't been used for a while.
     */
    public List<IndexSpec> getStaleIndexes(long staleThresholdMs) {
        long now = System.currentTimeMillis();
        List<IndexSpec> stale = new ArrayList<>();
        
        for (IndexSpec spec : indexSpecs.values()) {
            if ((now - spec.lastUsedAt) > staleThresholdMs) {
                stale.add(spec);
            }
        }
        
        return stale;
    }
    
    /**
     * Get catalog statistics.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalIndexes", indexSpecs.size());
        
        long totalHits = 0;
        long totalBuildCost = 0;
        long oldestCreated = Long.MAX_VALUE;
        long newestCreated = Long.MIN_VALUE;
        
        for (IndexSpec spec : indexSpecs.values()) {
            totalHits += spec.hitCount;
            totalBuildCost += spec.buildCost;
            oldestCreated = Math.min(oldestCreated, spec.createdAt);
            newestCreated = Math.max(newestCreated, spec.createdAt);
        }
        
        stats.put("totalHits", totalHits);
        stats.put("totalBuildCost", totalBuildCost);
        stats.put("oldestCreated", oldestCreated == Long.MAX_VALUE ? null : oldestCreated);
        stats.put("newestCreated", newestCreated == Long.MIN_VALUE ? null : newestCreated);
        
        return stats;
    }
    
    /**
     * Clear all index specifications (useful for testing).
     */
    public void clear() {
        indexSpecs.clear();
    }
    
    private String makeKey(String table, String column) {
        return table + ":" + column;
    }
    
    /**
     * Index specification with metadata.
     */
    public static class IndexSpec {
        public final String table;
        public final String column;
        public final long createdAt;
        public volatile long lastUsedAt;
        public volatile long hitCount = 0;
        public volatile long buildCost = 0; // Build time in milliseconds
        
        public IndexSpec(String table, String column, long createdAt) {
            this.table = table;
            this.column = column;
            this.createdAt = createdAt;
            this.lastUsedAt = createdAt;
        }
        
        @Override
        public String toString() {
            return String.format("IndexSpec{table='%s', column='%s', hits=%d, buildCost=%dms}", 
                table, column, hitCount, buildCost);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexSpec indexSpec = (IndexSpec) o;
            return Objects.equals(table, indexSpec.table) && Objects.equals(column, indexSpec.column);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(table, column);
        }
    }
}