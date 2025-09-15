import React from 'react';
import styled from 'styled-components';
import { Table, AlertCircle, Clock, CheckCircle } from 'lucide-react';
import { QueryResult } from '../App';

const ResultsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
`;

const ResultsHeader = styled.div`
  padding: 1rem;
  border-bottom: 1px solid #374151;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const ResultsTitle = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: #f3f4f6;
`;

const ExecutionInfo = styled.div`
  display: flex;
  align-items: center;
  gap: 1rem;
  font-size: 0.875rem;
  color: #9ca3af;
`;

const StatusBadge = styled.div<{ success: boolean }>`
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-size: 0.75rem;
  font-weight: 500;
  background: ${props => props.success ? '#065f46' : '#7f1d1d'};
  color: ${props => props.success ? '#10b981' : '#ef4444'};
`;

const ResultsContent = styled.div`
  flex: 1;
  overflow: auto;
  padding: 1rem;
`;

const TableContainer = styled.div`
  overflow: auto;
  border: 1px solid #374151;
  border-radius: 6px;
  background: #111827;
`;

const StyledTable = styled.table`
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
`;

const TableHeader = styled.thead`
  background: #1f2937;
  position: sticky;
  top: 0;
  z-index: 1;
`;

const HeaderCell = styled.th`
  padding: 0.75rem;
  text-align: left;
  font-weight: 600;
  color: #f3f4f6;
  border-bottom: 1px solid #374151;
  border-right: 1px solid #374151;
  
  &:last-child {
    border-right: none;
  }
`;

const TableBody = styled.tbody``;

const TableRow = styled.tr`
  &:nth-child(even) {
    background: #1f2937;
  }
  
  &:hover {
    background: #374151;
  }
`;

const TableCell = styled.td`
  padding: 0.75rem;
  border-bottom: 1px solid #374151;
  border-right: 1px solid #374151;
  color: #e5e7eb;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  
  &:last-child {
    border-right: none;
  }
`;

const EmptyState = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: #9ca3af;
  text-align: center;
`;

const ErrorMessage = styled.div`
  background: #7f1d1d;
  border: 1px solid #dc2626;
  border-radius: 6px;
  padding: 1rem;
  color: #fecaca;
  margin-bottom: 1rem;
`;

const RowCount = styled.div`
  padding: 0.5rem 1rem;
  background: #1f2937;
  border-top: 1px solid #374151;
  color: #9ca3af;
  font-size: 0.875rem;
`;

interface QueryResultsProps {
  result: QueryResult | null;
}

const QueryResults: React.FC<QueryResultsProps> = ({ result }) => {
  const formatValue = (value: any): string => {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    if (typeof value === 'object') {
      return JSON.stringify(value);
    }
    return String(value);
  };

  const formatExecutionTime = (time: number): string => {
    if (time < 1000) {
      return `${time}ms`;
    }
    return `${(time / 1000).toFixed(2)}s`;
  };

  return (
    <ResultsContainer>
      <ResultsHeader>
        <ResultsTitle>
          <Table size={18} />
          Query Results
        </ResultsTitle>
        {result && (
          <ExecutionInfo>
            <StatusBadge success={!result.error}>
              {result.error ? (
                <>
                  <AlertCircle size={12} />
                  Error
                </>
              ) : (
                <>
                  <CheckCircle size={12} />
                  Success
                </>
              )}
            </StatusBadge>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <Clock size={12} />
              {formatExecutionTime(result.executionTime)}
            </div>
          </ExecutionInfo>
        )}
      </ResultsHeader>

      <ResultsContent>
        {!result ? (
          <EmptyState>
            <Table size={48} style={{ marginBottom: '1rem', opacity: 0.5 }} />
            <p>Execute a SQL query to see results here</p>
            <p style={{ fontSize: '0.875rem', color: '#6b7280' }}>
              Use Cmd/Ctrl + Enter to run your query
            </p>
          </EmptyState>
        ) : result.error ? (
          <ErrorMessage>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
              <AlertCircle size={16} />
              <strong>Query Error</strong>
            </div>
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{result.error}</pre>
          </ErrorMessage>
        ) : result.rows.length === 0 ? (
          <EmptyState>
            <CheckCircle size={48} style={{ marginBottom: '1rem', color: '#10b981' }} />
            <p>Query executed successfully</p>
            <p style={{ fontSize: '0.875rem', color: '#6b7280' }}>
              No rows returned
            </p>
          </EmptyState>
        ) : (
          <>
            <TableContainer>
              <StyledTable>
                <TableHeader>
                  <tr>
                    {result.columns.map((column, index) => (
                      <HeaderCell key={index} title={column}>
                        {column}
                      </HeaderCell>
                    ))}
                  </tr>
                </TableHeader>
                <TableBody>
                  {result.rows.map((row, rowIndex) => (
                    <TableRow key={rowIndex}>
                      {row.map((cell, cellIndex) => (
                        <TableCell key={cellIndex} title={formatValue(cell)}>
                          {formatValue(cell)}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </StyledTable>
            </TableContainer>
            <RowCount>
              {result.rows.length} row{result.rows.length !== 1 ? 's' : ''} returned
            </RowCount>
          </>
        )}
      </ResultsContent>
    </ResultsContainer>
  );
};

export default QueryResults;