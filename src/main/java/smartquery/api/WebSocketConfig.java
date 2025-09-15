package smartquery.api;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.*;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new EventStreamHandler(), "/ws/events")
                .setAllowedOrigins("http://localhost:3000"); // Allow frontend origin
    }
}