package smartquery.api;

import org.springframework.web.socket.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.HashMap;

public class EventStreamHandler implements WebSocketHandler {
    
    private final Set<WebSocketSession> sessions = new CopyOnWriteArraySet<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        sessions.add(session);
        System.out.println("WebSocket connection established: " + session.getId());
        
        // Start sending periodic updates
        scheduler.scheduleAtFixedRate(() -> {
            try {
                sendEventUpdate(session);
            } catch (Exception e) {
                System.err.println("Error sending WebSocket update: " + e.getMessage());
            }
        }, 1, 5, TimeUnit.SECONDS); // Send update every 5 seconds
    }
    
    @Override
    public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
        // Handle incoming messages if needed
        System.out.println("Received WebSocket message: " + message.getPayload());
    }
    
    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        System.err.println("WebSocket transport error: " + exception.getMessage());
        sessions.remove(session);
    }
    
    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) throws Exception {
        sessions.remove(session);
        System.out.println("WebSocket connection closed: " + session.getId());
    }
    
    @Override
    public boolean supportsPartialMessages() {
        return false;
    }
    
    private void sendEventUpdate(WebSocketSession session) throws Exception {
        if (session.isOpen()) {
            Map<String, Object> event = new HashMap<>();
            event.put("id", System.currentTimeMillis());
            event.put("timestamp", java.time.Instant.now().toString());
            event.put("type", "SYSTEM");
            event.put("data", Map.of(
                "message", "Periodic system update",
                "activeConnections", sessions.size(),
                "uptime", System.currentTimeMillis()
            ));
            
            String eventJson = objectMapper.writeValueAsString(event);
            session.sendMessage(new TextMessage(eventJson));
        }
    }
    
    public void broadcastEvent(String eventType, Object eventData) {
        Map<String, Object> event = new HashMap<>();
        event.put("id", System.currentTimeMillis());
        event.put("timestamp", java.time.Instant.now().toString());
        event.put("type", eventType);
        event.put("data", eventData);
        
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            TextMessage message = new TextMessage(eventJson);
            
            for (WebSocketSession session : sessions) {
                if (session.isOpen()) {
                    try {
                        session.sendMessage(message);
                    } catch (Exception e) {
                        System.err.println("Failed to send message to session " + session.getId() + ": " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to serialize event: " + e.getMessage());
        }
    }
}