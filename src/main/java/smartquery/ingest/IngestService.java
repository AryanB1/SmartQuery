package smartquery.ingest;

import smartquery.ingest.model.Event;
import smartquery.storage.ColumnStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class IngestService {

    private final ColumnStore store;
    private final int batchSize;
    private final long flushMillis;

    private final List<Event> buffer;
    private final ScheduledExecutorService scheduler;
    private final AtomicInteger dropped;

    public IngestService(ColumnStore store) {
        this.store = store;
        this.batchSize = Integer.getInteger("ingest.batchSize", 10_000);
        this.flushMillis = Long.getLong("ingest.flushMillis", 500L);
        this.buffer = Collections.synchronizedList(new ArrayList<>(batchSize));
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ingest-flusher");
            t.setDaemon(true);
            return t;
        });
        this.dropped = new AtomicInteger();
        start(); // Auto-start the service
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::flushIfNeeded, flushMillis, flushMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
        flush(); // best-effort final flush
    }

    /** Accepts events from any source (HTTP, Kafka). Returns -1 if overloaded (soft drop). */
    public int submit(List<Event> events) {
        if (events == null || events.isEmpty()) return 0;
        synchronized (buffer) {
            // simple soft backpressure: drop if buffer is too large
            if (buffer.size() > 2 * batchSize) {
                dropped.addAndGet(events.size());
                return -1;
            }
            buffer.addAll(events);
            if (buffer.size() >= batchSize) flushLocked();
            return events.size();
        }
    }

    /** Flush current buffer into ColumnStore. */
    public void flush() {
        synchronized (buffer) {
            flushLocked();
        }
    }

    private void flushIfNeeded() {
        synchronized (buffer) {
            if (!buffer.isEmpty()) flushLocked();
        }
    }

    private void flushLocked() {
        if (buffer.isEmpty()) return;
        List<Event> batch = new ArrayList<>(buffer);
        buffer.clear();
        store.appendBatch(batch);
    }

    /** Expose minimal stats (also merges store stats). */
    public Map<String, Object> stats() {
        Map<String, Object> m = new HashMap<>();
        m.put("bufferSize", buffer.size());
        m.put("dropped", dropped.get());
        m.put("batchSize", batchSize);
        m.put("flushMillis", flushMillis);
        m.put("store", store.stats());
        return m;
    }

    // Query pass-through for manual testing and basic queries
    public Iterable<ColumnStore.Row> scan(String table, long fromTs, long toTs, Predicate<ColumnStore.Row> filter) {
        return store.scan(table, fromTs, toTs, filter);
    }
    
    /**
     * Simple query helper that returns raw events for a table within a time range.
     */
    public List<Event> queryEvents(String table, long fromTs, long toTs) {
        List<Event> events = new ArrayList<>();
        for (ColumnStore.Row row : store.scan(table, fromTs, toTs, null)) {
            events.add(row.getOriginalEvent());
        }
        return events;
    }
}