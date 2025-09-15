package smartquery.ingest.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

class EventTest {

    private Event event;
    private final long testTimestamp = Instant.now().toEpochMilli();

    @BeforeEach
    void setUp() {
        event = new Event();
    }

    @Test
    void testDefaultConstructor() {
        Event defaultEvent = new Event();
        
        assertNotNull(defaultEvent.props);
        assertEquals("events", defaultEvent.table);
        assertTrue(defaultEvent.ts > 0);
        assertTrue(Math.abs(defaultEvent.ts - System.currentTimeMillis()) < 1000); // Within 1 second
    }

    @Test
    void testSimpleConstructor() {
        Event simpleEvent = new Event("user123", "login");
        
        assertEquals("user123", simpleEvent.userId);
        assertEquals("login", simpleEvent.event);
        assertEquals("events", simpleEvent.table);
        assertNotNull(simpleEvent.props);
        assertTrue(simpleEvent.props.isEmpty());
    }

    @Test
    void testFullConstructor() {
        Map<String, String> props = new HashMap<>();
        props.put("ip", "192.168.1.1");
        props.put("browser", "Chrome");
        
        Event fullEvent = new Event(testTimestamp, "user_events", "user456", "page_view", props);
        
        assertEquals(testTimestamp, fullEvent.ts);
        assertEquals("user_events", fullEvent.table);
        assertEquals("user456", fullEvent.userId);
        assertEquals("page_view", fullEvent.event);
        assertEquals(props, fullEvent.props);
        assertEquals("192.168.1.1", fullEvent.getProperty("ip"));
        assertEquals("Chrome", fullEvent.getProperty("browser"));
    }

    @Test
    void testFullConstructorWithNullTable() {
        Event event = new Event(testTimestamp, null, "user123", "test", null);
        
        assertEquals("events", event.table); // Should default to "events"
        assertNotNull(event.props); // Should initialize empty map
    }

    @Test
    void testAddProperty() {
        event.addProperty("key1", "value1");
        event.addProperty("key2", "value2");
        
        assertEquals("value1", event.getProperty("key1"));
        assertEquals("value2", event.getProperty("key2"));
        assertEquals(2, event.props.size());
    }

    @Test
    void testAddPropertyChaining() {
        Event chainedEvent = new Event("user123", "test")
            .addProperty("prop1", "val1")
            .addProperty("prop2", "val2");
        
        assertEquals("val1", chainedEvent.getProperty("prop1"));
        assertEquals("val2", chainedEvent.getProperty("prop2"));
    }

    @Test
    void testGetPropertyWithNullProps() {
        Event eventWithNullProps = new Event(testTimestamp, "test", "user123", "event", null);
        eventWithNullProps.props = null; // Force null props
        
        assertNull(eventWithNullProps.getProperty("nonexistent"));
    }

    @Test
    void testGetPropertyNonExistent() {
        event.addProperty("existing", "value");
        
        assertEquals("value", event.getProperty("existing"));
        assertNull(event.getProperty("nonexistent"));
    }

    @Test
    void testIsValid() {
        // Valid event
        event.event = "login";
        assertTrue(event.isValid());
        
        // Invalid events
        event.event = null;
        assertFalse(event.isValid());
        
        event.event = "";
        assertFalse(event.isValid());
        
        event.event = "   ";
        assertFalse(event.isValid());
    }

    @Test
    void testEquals() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        
        Event event1 = new Event(testTimestamp, "table1", "user1", "event1", props);
        Event event2 = new Event(testTimestamp, "table1", "user1", "event1", props);
        Event event3 = new Event(testTimestamp, "table1", "user1", "event2", props);
        
        assertEquals(event1, event2);
        assertNotEquals(event1, event3);
        assertNotEquals(event1, null);
        assertNotEquals(event1, "not an event");
    }

    @Test
    void testHashCode() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        
        Event event1 = new Event(testTimestamp, "table1", "user1", "event1", props);
        Event event2 = new Event(testTimestamp, "table1", "user1", "event1", props);
        
        assertEquals(event1.hashCode(), event2.hashCode());
    }

    @Test
    void testToString() {
        event.ts = testTimestamp;
        event.table = "test_table";
        event.userId = "user123";
        event.event = "test_event";
        event.addProperty("prop1", "value1");
        
        String result = event.toString();
        
        assertTrue(result.contains("ts=" + testTimestamp));
        assertTrue(result.contains("table='test_table'"));
        assertTrue(result.contains("userId='user123'"));
        assertTrue(result.contains("event='test_event'"));
        assertTrue(result.contains("prop1=value1"));
    }

    @Test
    void testEqualsEdgeCases() {
        Event event1 = new Event("user1", "event1");
        Event event2 = new Event("user1", "event1");
        
        // Set same timestamp to make them equal
        event2.ts = event1.ts;
        
        assertEquals(event1, event2);
        assertEquals(event1, event1); // Self equality
        
        // Different timestamps
        event2.ts = event1.ts + 1000;
        assertNotEquals(event1, event2);
    }
}