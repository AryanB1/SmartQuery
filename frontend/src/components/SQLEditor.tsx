import React, { useState, useRef } from 'react';
import Editor from '@monaco-editor/react';
import styled from 'styled-components';
import { Play, Loader2, Code } from 'lucide-react';

const EditorContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
`;

const EditorHeader = styled.div`
  padding: 1rem;
  border-bottom: 1px solid #374151;
  display: flex;
  align-items: center;
  justify-content: space-between;
`;

const EditorTitle = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: #f3f4f6;
`;

const Controls = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
`;

const ExecuteButton = styled.button<{ disabled: boolean }>`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  background: ${props => props.disabled ? '#374151' : 'linear-gradient(90deg, #059669 0%, #10b981 100%)'};
  color: white;
  border: none;
  border-radius: 6px;
  cursor: ${props => props.disabled ? 'not-allowed' : 'pointer'};
  font-weight: 500;
  transition: all 0.2s;

  &:hover:not(:disabled) {
    background: linear-gradient(90deg, #047857 0%, #059669 100%);
    transform: translateY(-1px);
  }

  &:active:not(:disabled) {
    transform: translateY(0);
  }
`;

const EditorWrapper = styled.div`
  flex: 1;
  overflow: hidden;
`;

const defaultSQL = `-- Welcome to SmartQuery Dashboard!
-- SELECT text and press Execute, or place cursor on a statement

-- Simple select from events table
SELECT * FROM events LIMIT 10;

-- Get recent events with specific columns
SELECT userId, event, ts FROM events LIMIT 5;

-- Count events by user (GROUP BY aggregation)
SELECT userId, COUNT(*) as event_count
FROM events 
GROUP BY userId
LIMIT 10;

-- Get events by specific user
SELECT * FROM events WHERE userId = 'user_1' LIMIT 5;

-- Order events by timestamp (newest first)
SELECT userId, event, ts FROM events 
ORDER BY ts DESC LIMIT 10;

-- Use aggregate functions with GROUP BY
SELECT userId, MIN(ts) as first_event, MAX(ts) as last_event
FROM events 
GROUP BY userId;`;

interface SQLEditorProps {
  onExecute: (sql: string) => void;
  isExecuting: boolean;
  isConnected: boolean;
}

const SQLEditor: React.FC<SQLEditorProps> = ({ onExecute, isExecuting, isConnected }) => {
  const [sql, setSql] = useState(defaultSQL);
  const editorRef = useRef<any>(null);

  const handleExecute = () => {
    if (!isConnected || isExecuting || !sql.trim()) return;
    
    // Get selected text or find statement at cursor
    const editor = editorRef.current;
    let queryToExecute = sql;
    
    if (editor) {
      const selection = editor.getSelection();
      const selectedText = editor.getModel()?.getValueInRange(selection);
      
      if (selectedText && selectedText.trim()) {
        // Use selected text
        queryToExecute = selectedText;
      } else {
        // Find the SQL statement at cursor position
        const model = editor.getModel();
        const position = editor.getPosition();
        const allText = model.getValue();
        
        // Split by semicolons and find the statement containing the cursor
        const statements = allText.split(';');
        let currentLine = 1;
        
        for (const statement of statements) {
          const statementLines = statement.split('\n').length;
          if (position.lineNumber >= currentLine && position.lineNumber < currentLine + statementLines) {
            // Remove comments and trim
            const cleanStatement = statement
              .split('\n')
              .filter((line: string) => !line.trim().startsWith('--'))
              .join('\n')
              .trim();
            
            if (cleanStatement && cleanStatement.toUpperCase().startsWith('SELECT')) {
              queryToExecute = cleanStatement;
              break;
            }
          }
          currentLine += statementLines;
        }
        
        // If no statement found at cursor, try to find the first valid SELECT statement
        if (queryToExecute === sql) {
          for (const statement of statements) {
            const cleanStatement = statement
              .split('\n')
              .filter((line: string) => !line.trim().startsWith('--'))
              .join('\n')
              .trim();
            
            if (cleanStatement && cleanStatement.toUpperCase().startsWith('SELECT')) {
              queryToExecute = cleanStatement;
              break;
            }
          }
        }
      }
    }
    
    // Clean the query before execution
    queryToExecute = queryToExecute
      .split('\n')
      .filter((line: string) => !line.trim().startsWith('--'))
      .join('\n')
      .trim();
    
    if (!queryToExecute || !queryToExecute.toUpperCase().startsWith('SELECT')) {
      alert('Please select a valid SELECT statement to execute.');
      return;
    }
    
    onExecute(queryToExecute);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  };

  const handleEditorDidMount = (editor: any, monaco: any) => {
    editorRef.current = editor;
    
    // Add keyboard shortcut for execution
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      handleExecute();
    });
    
    // Focus the editor
    editor.focus();
  };

  return (
    <EditorContainer>
      <EditorHeader>
        <EditorTitle>
          <Code size={18} />
          SQL Editor
        </EditorTitle>
        <Controls>
          <ExecuteButton 
            onClick={handleExecute}
            disabled={!isConnected || isExecuting || !sql.trim()}
            title={!isConnected ? 'Database not connected' : 'Execute query (Cmd/Ctrl + Enter)'}
          >
            {isExecuting ? (
              <>
                <Loader2 size={16} className="animate-spin" />
                Executing...
              </>
            ) : (
              <>
                <Play size={16} />
                Execute
              </>
            )}
          </ExecuteButton>
        </Controls>
      </EditorHeader>
      
      <EditorWrapper onKeyDown={handleKeyDown}>
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={sql}
          onChange={(value) => setSql(value || '')}
          onMount={handleEditorDidMount}
          theme="vs-dark"
          options={{
            minimap: { enabled: false },
            fontSize: 13,
            lineNumbers: 'on',
            roundedSelection: false,
            scrollBeyondLastLine: false,
            automaticLayout: true,
            tabSize: 2,
            insertSpaces: true,
            wordWrap: 'on',
            contextmenu: true,
            selectOnLineNumbers: true,
            glyphMargin: false,
            folding: false,
            lineDecorationsWidth: 8,
            lineNumbersMinChars: 2,
            renderLineHighlight: 'line',
            overviewRulerLanes: 0,
            scrollbar: {
              vertical: 'auto',
              horizontal: 'auto',
              verticalScrollbarSize: 8,
              horizontalScrollbarSize: 8
            },
            bracketPairColorization: {
              enabled: true
            },
            suggest: {
              insertMode: 'replace'
            }
          }}
        />
      </EditorWrapper>
    </EditorContainer>
  );
};

export default SQLEditor;