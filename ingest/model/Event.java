package ingest.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an event in the system.
 * Events are the basic unit of data ingested into the SmartQuery database.
 */
public class Event {
    /** Timestamp in milliseconds since epoch */
    public long ts;
    
    /** Table/collection name for this event */
    public String table;
    
    /** User identifier associated with this event */
    public String userId;
    
    /** Event name/type */
    public String event;
    
    /** Additional properties for this event */
    public Map<String, String> props;

    /**
     * Default constructor with current timestamp and default table.
     */
    public Event() {
        this.ts = Instant.now().toEpochMilli();
        this.table = "events";
        this.props = new HashMap<>();
    }

    /**
     * Constructor with all fields.
     */
    public Event(long ts, String table, String userId, String event, Map<String, String> props) {
        this.ts = ts;
        this.table = table != null ? table : "events";
        this.userId = userId;
        this.event = event;
        this.props = props != null ? props : new HashMap<>();
    }

    /**
     * Quick constructor for simple events.
     */
    public Event(String userId, String event) {
        this();
        this.userId = userId;
        this.event = event;
    }

    /**
     * Add a property to this event.
     */
    public Event addProperty(String key, String value) {
        if (this.props == null) {
            this.props = new HashMap<>();
        }
        this.props.put(key, value);
        return this;
    }

    /**
     * Get a property value.
     */
    public String getProperty(String key) {
        return props != null ? props.get(key) : null;
    }

    /**
     * Check if this event is valid (has required fields).
     */
    public boolean isValid() {
        return event != null && !event.trim().isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event1 = (Event) o;
        return ts == event1.ts &&
                Objects.equals(table, event1.table) &&
                Objects.equals(userId, event1.userId) &&
                Objects.equals(event, event1.event) &&
                Objects.equals(props, event1.props);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ts, table, userId, event, props);
    }

    @Override
    public String toString() {
        return String.format("Event{ts=%d, table='%s', userId='%s', event='%s', props=%s}",
                ts, table, userId, event, props);
    }
}
