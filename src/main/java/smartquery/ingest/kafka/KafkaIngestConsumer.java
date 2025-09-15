package smartquery.ingest.kafka;

import smartquery.ingest.IngestService;
import smartquery.ingest.model.Event;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka consumer that reads JSON-serialized Event objects from Kafka topics.
 * Simplified version without Kafka dependencies for now.
 */
public class KafkaIngestConsumer {

    private final IngestService ingestService;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread consumerThread;

    public KafkaIngestConsumer(IngestService ingestService) {
        this.ingestService = ingestService;
        this.topic = System.getProperty("ingest.kafka.topic", "events");
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(this::runConsumerLoop, "kafka-ingest-consumer");
            consumerThread.setDaemon(true);
            consumerThread.start();
        }
    }

    public void stop() {
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(5000); // Wait up to 5 seconds for graceful shutdown
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runConsumerLoop() {
        // Placeholder implementation - in a real scenario this would connect to Kafka
        // For now, we'll simulate some events for testing
        try {
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                // Simulate receiving events
                Thread.sleep(5000); // Wait 5 seconds between batches
                
                // Create some test events
                List<Event> testEvents = createTestEvents();
                if (!testEvents.isEmpty()) {
                    ingestService.submit(testEvents);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (running.get()) {
                System.err.println("Kafka consumer error: " + e.getMessage());
            }
        }
    }

    private List<Event> createTestEvents() {
        List<Event> events = new ArrayList<>();
        
        // Create a few test events
        for (int i = 0; i < 3; i++) {
            Event event = new Event();
            event.userId = "user_" + (i + 1);
            event.event = "test_event_" + System.currentTimeMillis();
            event.table = topic;
            event.props.put("source", "kafka");
            event.props.put("batch", String.valueOf(System.currentTimeMillis() / 1000));
            events.add(event);
        }
        
        return events;
    }
}