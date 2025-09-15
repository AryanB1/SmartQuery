package smartquery.index;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Adaptive indexing using simple ML heuristics.
 * Observes query patterns and recommends indexes to build or drop.
 */
public class AdaptiveIndexML {
    
    // Configuration
    private final double w1_qps = 1.0;            // Weight for queries per second
    private final double w2_selectivity = 2.0;    // Weight for selectivity (1 - selectivity)
    private final double w3_equals = 1.5;         // Weight for equals predicates
    private final double w4_range = 2.0;          // Weight for range predicates  
    private final double w5_cardinality = 0.5;    // Penalty for high cardinality
    
    private final int observationWindowMs = 60_000; // 1 minute observation window
    private final int maxObservations = 1000;       // Keep last N observations per column
    
    // Query observations
    private final Map<String, List<QueryObservation>> columnObservations = new ConcurrentHashMap<>();
    
    /**
     * Observe a query that references a column.
     */
    public void observeQuery(String table, String column, boolean isRange, double selectivity) {
        String key = makeKey(table, column);
        long now = System.currentTimeMillis();
        
        QueryObservation observation = new QueryObservation(now, isRange, selectivity);
        
        columnObservations.compute(key, (k, observations) -> {
            if (observations == null) {
                observations = new ArrayList<>();
            }
            
            observations.add(observation);
            
            // Remove old observations beyond window
            observations.removeIf(obs -> (now - obs.timestamp) > observationWindowMs);
            
            // Limit size to prevent memory growth
            if (observations.size() > maxObservations) {
                observations.subList(0, observations.size() - maxObservations).clear();
            }
            
            return observations;
        });
    }
    
    /**
     * Recommend columns to build indexes for, within memory budget.
     */
    public List<String> recommendToBuild(String table, long memoryBudgetBytes, int maxNew) {
        List<ColumnScore> scores = new ArrayList<>();
        
        // Calculate scores for all observed columns
        for (Map.Entry<String, List<QueryObservation>> entry : columnObservations.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(table + ":")) {
                continue; // Different table
            }
            
            String column = key.substring(table.length() + 1);
            List<QueryObservation> observations = entry.getValue();
            
            if (observations.isEmpty()) {
                continue;
            }
            
            double score = calculateScore(observations);
            long estimatedMemory = estimateIndexMemory(column, observations);
            
            scores.add(new ColumnScore(column, score, estimatedMemory));
        }
        
        // Sort by score descending
        scores.sort((a, b) -> Double.compare(b.score, a.score));
        
        // Select top columns within budget
        List<String> recommendations = new ArrayList<>();
        long usedMemory = 0;
        
        for (ColumnScore columnScore : scores) {
            if (recommendations.size() >= maxNew) {
                break;
            }
            
            if (usedMemory + columnScore.estimatedMemory <= memoryBudgetBytes) {
                recommendations.add(columnScore.column);
                usedMemory += columnScore.estimatedMemory;
            }
        }
        
        return recommendations;
    }
    
    /**
     * Recommend columns to drop indexes for.
     */
    public List<String> recommendToDrop(String table, int maxDrop, long staleMs) {
        List<String> recommendations = new ArrayList<>();
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, List<QueryObservation>> entry : columnObservations.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(table + ":")) {
                continue; // Different table
            }
            
            String column = key.substring(table.length() + 1);
            List<QueryObservation> observations = entry.getValue();
            
            // Check if column is stale
            if (observations.isEmpty()) {
                recommendations.add(column);
            } else {
                // Find most recent observation
                long mostRecent = observations.stream()
                    .mapToLong(obs -> obs.timestamp)
                    .max()
                    .orElse(0);
                
                if ((now - mostRecent) > staleMs) {
                    recommendations.add(column);
                }
            }
            
            if (recommendations.size() >= maxDrop) {
                break;
            }
        }
        
        return recommendations;
    }
    
    /**
     * Calculate heuristic score for a column based on observations.
     */
    private double calculateScore(List<QueryObservation> observations) {
        if (observations.isEmpty()) {
            return 0.0;
        }
        
        // Calculate features
        double qps = calculateQps(observations);
        double avgSelectivity = calculateAvgSelectivity(observations);
        double equalsRatio = calculateEqualsRatio(observations);
        double rangeRatio = calculateRangeRatio(observations);
        double cardinality = estimateCardinality(observations);
        
        // Apply heuristic formula
        double score = w1_qps * qps
                     + w2_selectivity * (1.0 - avgSelectivity)
                     + w3_equals * equalsRatio
                     + w4_range * rangeRatio
                     - w5_cardinality * Math.log10(Math.max(1.0, cardinality));
        
        return Math.max(0.0, score);
    }
    
    private double calculateQps(List<QueryObservation> observations) {
        if (observations.isEmpty()) {
            return 0.0;
        }
        
        long windowMs = Math.min(observationWindowMs, 
            System.currentTimeMillis() - observations.get(0).timestamp);
        
        return (observations.size() * 1000.0) / Math.max(1.0, windowMs);
    }
    
    private double calculateAvgSelectivity(List<QueryObservation> observations) {
        return observations.stream()
            .mapToDouble(obs -> obs.selectivity)
            .average()
            .orElse(1.0);
    }
    
    private double calculateEqualsRatio(List<QueryObservation> observations) {
        long equalsCount = observations.stream()
            .mapToLong(obs -> obs.isRange ? 0 : 1)
            .sum();
        
        return (double) equalsCount / observations.size();
    }
    
    private double calculateRangeRatio(List<QueryObservation> observations) {
        long rangeCount = observations.stream()
            .mapToLong(obs -> obs.isRange ? 1 : 0)
            .sum();
        
        return (double) rangeCount / observations.size();
    }
    
    private double estimateCardinality(List<QueryObservation> observations) {
        // Simple cardinality estimation based on selectivity
        // Lower average selectivity suggests higher cardinality
        double avgSelectivity = calculateAvgSelectivity(observations);
        return Math.max(1.0, 1.0 / Math.max(0.001, avgSelectivity));
    }
    
    private long estimateIndexMemory(String column, List<QueryObservation> observations) {
        // Simple memory estimation
        // Bitmap indexes: ~100 bytes per distinct value
        // B-tree indexes: ~20 bytes per row
        
        boolean isNumeric = column.equals("ts") || 
                           (column.startsWith("props.") && looksNumeric(observations));
        
        if (isNumeric) {
            // B-tree index
            return observations.size() * 20;
        } else {
            // Bitmap index
            double cardinality = estimateCardinality(observations);
            return (long) (cardinality * 100);
        }
    }
    
    private boolean looksNumeric(List<QueryObservation> observations) {
        // If we see range queries, it's probably numeric
        return observations.stream().anyMatch(obs -> obs.isRange);
    }
    
    private String makeKey(String table, String column) {
        return table + ":" + column;
    }
    
    /**
     * Get statistics about the adaptive indexing system.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("trackedColumns", columnObservations.size());
        
        int totalObservations = columnObservations.values().stream()
            .mapToInt(List::size)
            .sum();
        stats.put("totalObservations", totalObservations);
        
        // Calculate average QPS across all columns
        double totalQps = 0.0;
        for (List<QueryObservation> observations : columnObservations.values()) {
            totalQps += calculateQps(observations);
        }
        stats.put("totalQps", totalQps);
        
        return stats;
    }
    
    /**
     * Clear all observations (useful for testing).
     */
    public void clear() {
        columnObservations.clear();
    }
    
    /**
     * Single query observation.
     */
    private static class QueryObservation {
        final long timestamp;
        final boolean isRange;
        final double selectivity;
        
        QueryObservation(long timestamp, boolean isRange, double selectivity) {
            this.timestamp = timestamp;
            this.isRange = isRange;
            this.selectivity = Math.max(0.0, Math.min(1.0, selectivity)); // Clamp to [0,1]
        }
    }
    
    /**
     * Column score for ranking.
     */
    private static class ColumnScore {
        final String column;
        final double score;
        final long estimatedMemory;
        
        ColumnScore(String column, double score, long estimatedMemory) {
            this.column = column;
            this.score = score;
            this.estimatedMemory = estimatedMemory;
        }
        
        @Override
        public String toString() {
            return String.format("ColumnScore{column='%s', score=%.2f, memory=%d}", 
                column, score, estimatedMemory);
        }
    }
}