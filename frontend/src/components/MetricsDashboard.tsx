import React from 'react';
import styled from 'styled-components';
import { BarChart3, Database, Clock, Cpu, HardDrive } from 'lucide-react';
import type { MetricsData } from '../App';

const MetricsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
`;

const MetricsHeader = styled.div`
  padding: 1rem;
  border-bottom: 1px solid #374151;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: #f3f4f6;
`;

const MetricsContent = styled.div`
  flex: 1;
  padding: 1rem;
  overflow-y: auto;
`;

const MetricCard = styled.div`
  background: #111827;
  border: 1px solid #374151;
  border-radius: 6px;
  padding: 1rem;
  margin-bottom: 0.75rem;
  transition: all 0.2s;

  &:hover {
    border-color: #4b5563;
    background: #1f2937;
  }

  &:last-child {
    margin-bottom: 0;
  }
`;

const MetricHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
`;

const MetricIcon = styled.div<{ color: string }>`
  color: ${props => props.color};
`;

const MetricLabel = styled.div`
  font-size: 0.875rem;
  color: #9ca3af;
  font-weight: 500;
`;

const MetricValue = styled.div`
  font-size: 1.5rem;
  font-weight: bold;
  color: #f3f4f6;
  margin-bottom: 0.25rem;
`;

const MetricSubtext = styled.div`
  font-size: 0.75rem;
  color: #6b7280;
`;

const ProgressBar = styled.div`
  width: 100%;
  height: 4px;
  background: #374151;
  border-radius: 2px;
  overflow: hidden;
  margin-top: 0.5rem;
`;

const ProgressFill = styled.div<{ percentage: number; color: string }>`
  height: 100%;
  width: ${props => props.percentage}%;
  background: ${props => props.color};
  transition: width 0.3s ease;
`;

const StatusDot = styled.div<{ color: string }>`
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: ${props => props.color};
  margin-right: 0.5rem;
`;

interface MetricsDashboardProps {
  metrics: MetricsData;
}

const MetricsDashboard: React.FC<MetricsDashboardProps> = ({ metrics }) => {
  const formatNumber = (num: number): string => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    }
    if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toString();
  };

  const formatTime = (ms: number): string => {
    if (ms < 1000) {
      return `${ms.toFixed(0)}ms`;
    }
    return `${(ms / 1000).toFixed(2)}s`;
  };

  const formatMemory = (bytes: number): string => {
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }
    
    return `${size.toFixed(1)} ${units[unitIndex]}`;
  };

  const getPerformanceColor = (value: number, thresholds: { good: number; warning: number }): string => {
    if (value <= thresholds.good) return '#10b981';
    if (value <= thresholds.warning) return '#f59e0b';
    return '#ef4444';
  };

  const memoryPercentage = Math.min((metrics.memoryUsage / (1024 * 1024 * 1024)) * 100, 100); // Assume 1GB max
  const loadPercentage = Math.min(metrics.systemLoad * 100, 100);

  return (
    <MetricsContainer>
      <MetricsHeader>
        <BarChart3 size={18} />
        System Metrics
      </MetricsHeader>

      <MetricsContent>
        <MetricCard>
          <MetricHeader>
            <MetricIcon color="#10b981">
              <Database size={16} />
            </MetricIcon>
            <MetricLabel>Total Events</MetricLabel>
          </MetricHeader>
          <MetricValue>{formatNumber(metrics.totalEvents)}</MetricValue>
          <MetricSubtext>Events processed</MetricSubtext>
        </MetricCard>

        <MetricCard>
          <MetricHeader>
            <MetricIcon color="#3b82f6">
              <BarChart3 size={16} />
            </MetricIcon>
            <MetricLabel>Queries Executed</MetricLabel>
          </MetricHeader>
          <MetricValue>{formatNumber(metrics.queriesExecuted)}</MetricValue>
          <MetricSubtext>SQL queries run</MetricSubtext>
        </MetricCard>

        <MetricCard>
          <MetricHeader>
            <MetricIcon color={getPerformanceColor(metrics.averageQueryTime, { good: 100, warning: 500 })}>
              <Clock size={16} />
            </MetricIcon>
            <MetricLabel>Avg Query Time</MetricLabel>
          </MetricHeader>
          <MetricValue>{formatTime(metrics.averageQueryTime)}</MetricValue>
          <MetricSubtext>Response time</MetricSubtext>
        </MetricCard>

        <MetricCard>
          <MetricHeader>
            <MetricIcon color={getPerformanceColor(memoryPercentage, { good: 50, warning: 80 })}>
              <HardDrive size={16} />
            </MetricIcon>
            <MetricLabel>Memory Usage</MetricLabel>
          </MetricHeader>
          <MetricValue>{formatMemory(metrics.memoryUsage)}</MetricValue>
          <ProgressBar>
            <ProgressFill 
              percentage={memoryPercentage} 
              color={getPerformanceColor(memoryPercentage, { good: 50, warning: 80 })}
            />
          </ProgressBar>
          <MetricSubtext>{memoryPercentage.toFixed(1)}% of system memory</MetricSubtext>
        </MetricCard>

        <MetricCard>
          <MetricHeader>
            <MetricIcon color={getPerformanceColor(loadPercentage, { good: 50, warning: 80 })}>
              <Cpu size={16} />
            </MetricIcon>
            <MetricLabel>System Load</MetricLabel>
          </MetricHeader>
          <MetricValue>{(metrics.systemLoad * 100).toFixed(1)}%</MetricValue>
          <ProgressBar>
            <ProgressFill 
              percentage={loadPercentage} 
              color={getPerformanceColor(loadPercentage, { good: 50, warning: 80 })}
            />
          </ProgressBar>
          <MetricSubtext>CPU utilization</MetricSubtext>
        </MetricCard>
      </MetricsContent>
    </MetricsContainer>
  );
};

export default MetricsDashboard;