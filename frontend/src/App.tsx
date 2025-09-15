import React, { useState, useEffect } from 'react';
import styled from 'styled-components';
import { Database, Activity, Square } from 'lucide-react';
import SQLEditor from './components/SQLEditor';
import QueryResults from './components/QueryResults';
import EventStream from './components/EventStream';
import MetricsDashboard from './components/MetricsDashboard';
import './App.css';

const AppContainer = styled.div`
  min-height: 100vh;
  display: flex;
  flex-direction: column;
  background: #0f1419;
  color: #ffffff;
  font-family: 'SF Mono', Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, 'Courier New', monospace;
`;

const Header = styled.header`
  background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
  padding: 1rem 2rem;
  display: flex;
  align-items: center;
  justify-content: space-between;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
`;

const Logo = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 1.5rem;
  font-weight: bold;
`;

const StatusIndicator = styled.div<{ connected: boolean }>`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  color: ${props => props.connected ? '#10b981' : '#ef4444'};
`;

const MainContent = styled.main`
  flex: 1;
  display: grid;
  grid-template-columns: 2fr 350px;
  grid-template-rows: auto;
  gap: 1rem;
  padding: 1rem;
  min-height: 0;
  
  @media (max-width: 1200px) {
    grid-template-columns: 1fr;
    grid-template-rows: auto auto;
  }
`;

const EditorPanel = styled.div`
  background: #1e293b;
  border-radius: 8px;
  border: 1px solid #374151;
  display: flex;
  flex-direction: column;
  height: 300px;
  margin-bottom: 1rem;
`;

const ResultsPanel = styled.div`
  background: #1e293b;
  border-radius: 8px;
  border: 1px solid #374151;
  display: flex;
  flex-direction: column;
  height: 500px;
`;

const LeftColumn = styled.div`
  display: flex;
  flex-direction: column;
`;

const SidePanel = styled.div`
  display: flex;
  flex-direction: column;
  gap: 1rem;
  
  @media (max-width: 1200px) {
    flex-direction: row;
  }
  
  @media (max-width: 768px) {
    flex-direction: column;
  }
`;

const Panel = styled.div`
  background: #1e293b;
  border-radius: 8px;
  border: 1px solid #374151;
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
`;

export interface QueryResult {
  columns: string[];
  rows: any[][];
  executionTime: number;
  error?: string;
}

export interface EventData {
  id: string;
  timestamp: string;
  type: string;
  data: any;
}

export interface MetricsData {
  totalEvents: number;
  queriesExecuted: number;
  averageQueryTime: number;
  memoryUsage: number;
  systemLoad: number;
}

function App() {
  const [isConnected, setIsConnected] = useState(false);
  const [isExecuting, setIsExecuting] = useState(false);
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);
  const [events, setEvents] = useState<EventData[]>([
    {
      id: '1',
      timestamp: new Date().toISOString(),
      type: 'QUERY',
      data: { sql: 'SELECT * FROM events LIMIT 5', duration: '10ms' }
    },
    {
      id: '2',
      timestamp: new Date(Date.now() - 30000).toISOString(),
      type: 'INSERT',
      data: { table: 'events', rows: 1 }
    }
  ]);
  const [metrics, setMetrics] = useState<MetricsData>({
    totalEvents: 0,
    queriesExecuted: 0,
    averageQueryTime: 0,
    memoryUsage: 0,
    systemLoad: 0
  });

  useEffect(() => {
    // Check backend connection
    checkConnection();
    const interval = setInterval(checkConnection, 5000);
    
    // Fetch metrics periodically
    if (isConnected) {
      fetchMetrics();
      const metricsInterval = setInterval(fetchMetrics, 10000); // Every 10 seconds
      return () => {
        clearInterval(interval);
        clearInterval(metricsInterval);
      };
    }
    
    return () => clearInterval(interval);
  }, [isConnected]);

  const checkConnection = async () => {
    try {
      console.log('Checking connection to backend...');
      const response = await fetch('http://localhost:8080/metrics/health');
      console.log('Backend response:', response.status, response.ok);
      setIsConnected(response.ok);
    } catch (error) {
      console.error('Connection failed:', error);
      setIsConnected(false);
    }
  };

  const handleNewEvent = (newEvent: EventData) => {
    setEvents(prev => [...prev.slice(-49), newEvent]); // Keep only last 50 events
  };

  const fetchMetrics = async () => {
    try {
      const response = await fetch('http://localhost:8080/api/metrics');
      if (response.ok) {
        const data = await response.json();
        setMetrics({
          totalEvents: data.totalEvents || 0,
          queriesExecuted: data.queriesExecuted || 0,
          averageQueryTime: data.averageQueryTime || 0,
          memoryUsage: data.memoryUsage || 0,
          systemLoad: data.systemLoad || 0,
        });
      }
    } catch (error) {
      console.error('Failed to fetch metrics:', error);
    }
  };

  const executeQuery = async (sql: string) => {
    setIsExecuting(true);
    try {
      console.log('Executing query:', sql);
      const startTime = Date.now();
      const response = await fetch('http://localhost:8080/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ sql }),
      });

      console.log('Query response status:', response.status, response.ok);
      const data = await response.json();
      console.log('Query response data:', data);
      const executionTime = Date.now() - startTime;

      if (response.ok) {
        setQueryResult({
          columns: data.columns || [],
          rows: data.rows || [],
          executionTime,
        });
      } else {
        setQueryResult({
          columns: [],
          rows: [],
          executionTime,
          error: data.error || 'Query execution failed',
        });
      }
    } catch (error) {
      console.error('Query execution error:', error);
      setQueryResult({
        columns: [],
        rows: [],
        executionTime: 0,
        error: error instanceof Error ? error.message : 'Network error',
      });
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <AppContainer>
      <Header>
        <Logo>
          <Database size={28} />
          <span>SmartQuery Dashboard</span>
        </Logo>
        <StatusIndicator connected={isConnected}>
          {isConnected ? (
            <>
              <Activity size={16} />
              <span>Connected</span>
            </>
          ) : (
            <>
              <Square size={16} />
              <span>Disconnected</span>
            </>
          )}
        </StatusIndicator>
      </Header>

      <MainContent>
        <LeftColumn>
          <EditorPanel>
            <SQLEditor 
              onExecute={executeQuery} 
              isExecuting={isExecuting}
              isConnected={isConnected}
            />
          </EditorPanel>

          <ResultsPanel>
            <QueryResults result={queryResult} />
          </ResultsPanel>
        </LeftColumn>

        <SidePanel>
          <Panel style={{ height: '280px' }}>
            <MetricsDashboard metrics={metrics} />
          </Panel>
          
          <Panel style={{ height: '500px' }}>
            <EventStream events={events} onNewEvent={handleNewEvent} />
          </Panel>
        </SidePanel>
      </MainContent>
    </AppContainer>
  );
}

export default App;
