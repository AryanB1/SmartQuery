package smartquery.storage;

import smartquery.ingest.model.Event;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * In-memory columnar storage engine for event data.
 * Groups events by table and provides efficient querying capabilities.
 */
public class ColumnStore {
    
    // Table name -> List of events for that table
    private final Map<String, List<Event>> tables = new ConcurrentHashMap<>();
    private final AtomicLong totalEvents = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    
    /**
     * Append a batch of events to the store.
     */
    public void appendBatch(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        
        // Group events by table
        Map<String, List<Event>> eventsByTable = new HashMap<>();
        for (Event event : events) {
            String tableName = event.table != null ? event.table : "events";
            eventsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(event);
        }
        
        // Add to respective tables
        for (Map.Entry<String, List<Event>> entry : eventsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<Event> tableEvents = entry.getValue();
            
            tables.computeIfAbsent(tableName, k -> Collections.synchronizedList(new ArrayList<>()))
                  .addAll(tableEvents);
        }
        
        totalEvents.addAndGet(events.size());
        totalBatches.incrementAndGet();
    }
    
    /**
     * @deprecated Use appendBatch instead
     */
    @Deprecated
    public void addAll(List<Event> newEvents) {
        appendBatch(newEvents);
    }
    
    /**
     * Scan events from a specific table within a time range.
     */
    public Iterable<Row> scan(String table, long fromTs, long toTs, Predicate<Row> filter) {
        List<Event> tableEvents = tables.get(table);
        if (tableEvents == null) {
            return Collections.emptyList();
        }
        
        List<Row> result = new ArrayList<>();
        synchronized (tableEvents) {
            for (Event event : tableEvents) {
                if (event.ts >= fromTs && event.ts <= toTs) {
                    Row row = new Row(event);
                    if (filter == null || filter.test(row)) {
                        result.add(row);
                    }
                }
            }
        }
        return result;
    }
    
    /**
     * Get storage statistics.
     */
    public Map<String, Object> stats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalEvents", totalEvents.get());
        stats.put("totalBatches", totalBatches.get());
        stats.put("tablesCount", tables.size());
        
        Map<String, Integer> tableSizes = new HashMap<>();
        for (Map.Entry<String, List<Event>> entry : tables.entrySet()) {
            tableSizes.put(entry.getKey(), entry.getValue().size());
        }
        stats.put("tableSizes", tableSizes);
        
        return stats;
    }
    
    /**
     * Get total number of events across all tables.
     */
    public int size() {
        return (int) totalEvents.get();
    }
    
    /**
     * Get list of table names.
     */
    public List<String> getTableNames() {
        return new ArrayList<>(tables.keySet());
    }
    
    /**
     * Clear all data (useful for testing).
     */
    public void clear() {
        tables.clear();
        totalEvents.set(0);
        totalBatches.set(0);
    }
    
    /**
     * Row wrapper for Event data to provide a consistent query interface.
     */
    public static class Row {
        private final Event event;
        
        public Row(Event event) {
            this.event = event;
        }
        
        public long getTimestamp() {
            return event.ts;
        }
        
        public String getTable() {
            return event.table;
        }
        
        public String getUserId() {
            return event.userId;
        }
        
        public String getEvent() {
            return event.event;
        }
        
        public Map<String, String> getProps() {
            return event.props;
        }
        
        public String getProperty(String key) {
            return event.props != null ? event.props.get(key) : null;
        }
        
        public Event getOriginalEvent() {
            return event;
        }
        
        @Override
        public String toString() {
            return String.format("Row{ts=%d, table=%s, userId=%s, event=%s, props=%s}", 
                event.ts, event.table, event.userId, event.event, event.props);
        }
    }
}