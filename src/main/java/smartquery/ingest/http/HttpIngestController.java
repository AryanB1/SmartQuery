package smartquery.ingest.http;

import smartquery.ingest.model.Event;
import smartquery.ingest.IngestService;
import java.util.List;
import java.util.Map;

/**
 * REST controller for HTTP-based event ingestion.
 * Provides endpoints for submitting events and checking system status.
 */
public class HttpIngestController {

    private final IngestService ingestService;

    public HttpIngestController(IngestService ingestService) {
        this.ingestService = ingestService;
    }

    /**
     * Submit a batch of events for ingestion.
     * 
     * @param events List of events to ingest
     * @return Response indicating success or failure
     */
    public String ingestEvents(List<Event> events) {
        if (events == null || events.isEmpty()) {
            return "error: empty event list";
        }

        // Validate events
        long invalidEvents = events.stream().filter(e -> !e.isValid()).count();
        if (invalidEvents > 0) {
            return "error: " + invalidEvents + " invalid events (missing event name)";
        }

        int accepted = ingestService.submit(events);
        if (accepted >= 0) {
            return "success: accepted " + accepted + " events";
        } else {
            return "error: system overloaded, events dropped";
        }
    }

    /**
     * Get system statistics.
     * 
     * @return Map containing various system metrics
     */
    public Map<String, Object> getStats() {
        return ingestService.stats();
    }

    /**
     * Health check endpoint.
     * 
     * @return Simple health status
     */
    public String getHealth() {
        Map<String, Object> stats = ingestService.stats();
        Integer bufferSize = (Integer) stats.get("bufferSize");
        Integer dropped = (Integer) stats.get("dropped");
        
        if (bufferSize != null && bufferSize > 50000) {
            return "unhealthy: buffer too full (" + bufferSize + ")";
        }
        if (dropped != null && dropped > 1000) {
            return "unhealthy: too many dropped events (" + dropped + ")";
        }
        
        return "healthy";
    }
}