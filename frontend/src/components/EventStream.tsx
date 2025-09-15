import React, { useEffect, useRef } from 'react';
import styled from 'styled-components';
import { Zap, Circle, Pause, Play } from 'lucide-react';
import type { EventData } from '../App';

const StreamContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
`;

const StreamHeader = styled.div`
  padding: 1rem;
  border-bottom: 1px solid #374151;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const StreamTitle = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: #f3f4f6;
`;

const StreamControls = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
`;

const ControlButton = styled.button<{ active?: boolean }>`
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.25rem 0.5rem;
  background: ${props => props.active ? '#065f46' : 'transparent'};
  color: ${props => props.active ? '#10b981' : '#9ca3af'};
  border: 1px solid ${props => props.active ? '#10b981' : '#374151'};
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.75rem;
  transition: all 0.2s;

  &:hover {
    background: ${props => props.active ? '#047857' : '#374151'};
    color: ${props => props.active ? '#10b981' : '#f3f4f6'};
  }
`;

const StreamContent = styled.div`
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
`;

const EventList = styled.div`
  flex: 1;
  overflow-y: auto;
  padding: 0.5rem;
  scrollbar-width: thin;
  scrollbar-color: #374151 transparent;
  
  &::-webkit-scrollbar {
    width: 6px;
  }
  
  &::-webkit-scrollbar-track {
    background: transparent;
  }
  
  &::-webkit-scrollbar-thumb {
    background: #374151;
    border-radius: 3px;
  }
`;

const EventItem = styled.div<{ type: string }>`
  display: flex;
  align-items: flex-start;
  gap: 0.75rem;
  padding: 0.75rem;
  margin-bottom: 0.5rem;
  background: #1f2937;
  border-radius: 6px;
  border-left: 3px solid ${props => getEventColor(props.type)};
  font-size: 0.875rem;
  transition: all 0.2s;

  &:hover {
    background: #374151;
  }

  &:last-child {
    margin-bottom: 0;
  }
`;

const EventIndicator = styled.div<{ type: string }>`
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: ${props => getEventColor(props.type)};
  margin-top: 0.25rem;
  flex-shrink: 0;
`;

const EventContent = styled.div`
  flex: 1;
  min-width: 0;
`;

const EventHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 0.25rem;
`;

const EventType = styled.span<{ type: string }>`
  font-weight: 600;
  color: ${props => getEventColor(props.type)};
  text-transform: uppercase;
  font-size: 0.75rem;
`;

const EventTime = styled.span`
  color: #9ca3af;
  font-size: 0.75rem;
`;

const EventData = styled.div`
  color: #e5e7eb;
  word-break: break-word;
`;

const EmptyState = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #9ca3af;
  text-align: center;
  padding: 2rem;
`;

const ConnectionStatus = styled.div<{ connected: boolean }>`
  padding: 0.5rem 1rem;
  background: ${props => props.connected ? '#065f46' : '#7f1d1d'};
  color: ${props => props.connected ? '#10b981' : '#ef4444'};
  font-size: 0.75rem;
  text-align: center;
  border-top: 1px solid #374151;
`;

function getEventColor(type: string): string {
  const colors: { [key: string]: string } = {
    'INSERT': '#10b981',
    'UPDATE': '#3b82f6',
    'DELETE': '#ef4444',
    'QUERY': '#8b5cf6',
    'SYSTEM': '#f59e0b',
    'ERROR': '#ef4444',
    'default': '#6b7280'
  };
  return colors[type.toUpperCase()] || colors.default;
}

interface EventStreamProps {
  events: EventData[];
  onNewEvent?: (event: EventData) => void;
}

const EventStream: React.FC<EventStreamProps> = ({ events, onNewEvent }) => {
  const [isConnected, setIsConnected] = React.useState(false);
  const [isPaused, setIsPaused] = React.useState(false);
  const eventListRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    // Auto-scroll to bottom when new events arrive
    if (eventListRef.current && !isPaused) {
      eventListRef.current.scrollTop = eventListRef.current.scrollHeight;
    }
  }, [events, isPaused]);

  useEffect(() => {
    // Connect to WebSocket for real-time events
    const connectWebSocket = () => {
      try {
        wsRef.current = new WebSocket('ws://localhost:8080/ws/events');
        
        wsRef.current.onopen = () => {
          console.log('WebSocket connected');
          setIsConnected(true);
        };
        
        wsRef.current.onclose = () => {
          console.log('WebSocket disconnected');
          setIsConnected(false);
          // Attempt to reconnect after 3 seconds
          setTimeout(connectWebSocket, 3000);
        };
        
        wsRef.current.onerror = (error) => {
          console.error('WebSocket error:', error);
          setIsConnected(false);
        };
        
        wsRef.current.onmessage = (event) => {
          try {
            const eventData = JSON.parse(event.data);
            if (!isPaused && onNewEvent) {
              onNewEvent(eventData);
            }
          } catch (error) {
            console.error('Failed to parse WebSocket message:', error);
          }
        };
        
      } catch (error) {
        console.error('Failed to connect WebSocket:', error);
        setIsConnected(false);
      }
    };

    connectWebSocket();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [isPaused]);

  const formatTimestamp = (timestamp: string): string => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', { 
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const formatEventData = (data: any): string => {
    if (typeof data === 'string') {
      return data;
    }
    if (typeof data === 'object') {
      return JSON.stringify(data, null, 2);
    }
    return String(data);
  };

  const togglePause = () => {
    setIsPaused(!isPaused);
  };

  return (
    <StreamContainer>
      <StreamHeader>
        <StreamTitle>
          <Zap size={18} />
          Event Stream
        </StreamTitle>
        <StreamControls>
          <ControlButton active={!isPaused} onClick={togglePause}>
            {isPaused ? <Play size={12} /> : <Pause size={12} />}
            {isPaused ? 'Resume' : 'Pause'}
          </ControlButton>
        </StreamControls>
      </StreamHeader>

      <StreamContent>
        {events.length === 0 ? (
          <EmptyState>
            <Circle size={48} style={{ marginBottom: '1rem', opacity: 0.5 }} />
            <p>Waiting for events...</p>
            <p style={{ fontSize: '0.75rem', color: '#6b7280' }}>
              Real-time events will appear here
            </p>
          </EmptyState>
        ) : (
          <EventList ref={eventListRef}>
            {events.map((event) => (
              <EventItem key={event.id} type={event.type}>
                <EventIndicator type={event.type} />
                <EventContent>
                  <EventHeader>
                    <EventType type={event.type}>{event.type}</EventType>
                    <EventTime>{formatTimestamp(event.timestamp)}</EventTime>
                  </EventHeader>
                  <EventData>
                    <pre style={{ margin: 0, fontSize: '0.75rem', whiteSpace: 'pre-wrap' }}>
                      {formatEventData(event.data)}
                    </pre>
                  </EventData>
                </EventContent>
              </EventItem>
            ))}
          </EventList>
        )}
      </StreamContent>

      <ConnectionStatus connected={isConnected}>
        {isConnected ? '● Connected to event stream' : '○ Disconnected from event stream'}
      </ConnectionStatus>
    </StreamContainer>
  );
};

export default EventStream;