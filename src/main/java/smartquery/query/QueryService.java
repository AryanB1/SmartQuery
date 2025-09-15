package smartquery.query;

import smartquery.storage.ColumnStore;
import org.springframework.stereotype.Service;

/**
 * Query service that orchestrates the complete query processing pipeline.
 * Provides the main entry point for executing SQL queries: parse → plan → execute.
 */
@Service
public class QueryService {
    
    private final ColumnStore store;
    private final Executor executor;
    
    /**
     * Constructor with column store dependency.
     */
    public QueryService(ColumnStore store) {
        this.store = store;
        this.executor = new Executor(store);
    }
    
    /**
     * Execute a SQL query and return results.
     * This is the main entry point for query execution.
     */
    public QueryApi.QueryResult executeQuery(QueryApi.QueryRequest request) 
            throws QueryExceptions.ParseException, QueryExceptions.PlanningException {
        
        if (request == null || request.sql == null || request.sql.trim().isEmpty()) {
            throw new QueryExceptions.ParseException("Empty or null SQL query");
        }
        
        try {
            // Phase 1: Parse SQL to AST
            Ast.Statement statement = parseSql(request.sql);
            
            // Phase 2: Plan query (logical → physical)
            Planner.PhysicalPlan plan = planQuery(statement, request);
            
            // Phase 3: Execute physical plan
            QueryApi.QueryResult result = executePhysicalPlan(plan, request);
            
            return result;
            
        } catch (QueryExceptions.ParseException | QueryExceptions.PlanningException e) {
            // Re-throw planning and parsing exceptions
            throw e;
        } catch (QueryExceptions.ExecutionException e) {
            // Wrap execution exceptions as runtime exceptions are expected to bubble up
            throw e;
        } catch (Exception e) {
            // Wrap any other unexpected exceptions
            throw new QueryExceptions.ExecutionException("Unexpected error during query execution", e);
        }
    }
    
    /**
     * Execute a SQL query with just the SQL string (convenience method).
     */
    public QueryApi.QueryResult executeQuery(String sql) 
            throws QueryExceptions.ParseException, QueryExceptions.PlanningException {
        return executeQuery(new QueryApi.QueryRequest(sql));
    }
    
    /**
     * Execute a SQL query with SQL and limit hint.
     */
    public QueryApi.QueryResult executeQuery(String sql, Integer limitHint) 
            throws QueryExceptions.ParseException, QueryExceptions.PlanningException {
        return executeQuery(new QueryApi.QueryRequest(sql, limitHint));
    }
    
    /**
     * Parse SQL string to AST.
     */
    private Ast.Statement parseSql(String sql) throws QueryExceptions.ParseException {
        try {
            return Sql.parse(sql);
        } catch (RuntimeException e) {
            // Convert ANTLR runtime exceptions to our ParseException
            throw new QueryExceptions.ParseException("Failed to parse SQL: " + e.getMessage(), e);
        }
    }
    
    /**
     * Plan query from AST.
     */
    private Planner.PhysicalPlan planQuery(Ast.Statement statement, QueryApi.QueryRequest request) 
            throws QueryExceptions.PlanningException {
        return Planner.plan(statement, request);
    }
    
    /**
     * Execute physical plan.
     */
    private QueryApi.QueryResult executePhysicalPlan(Planner.PhysicalPlan plan, QueryApi.QueryRequest request) {
        return executor.execute(plan, request);
    }
    
    /**
     * Validate SQL syntax without executing (useful for query validation).
     */
    public void validateSql(String sql) throws QueryExceptions.ParseException {
        if (sql == null || sql.trim().isEmpty()) {
            throw new QueryExceptions.ParseException("Empty or null SQL query");
        }
        
        try {
            Sql.parse(sql);
        } catch (RuntimeException e) {
            throw new QueryExceptions.ParseException("Invalid SQL syntax: " + e.getMessage(), e);
        }
    }
    
    /**
     * Get query execution plan without executing (useful for debugging).
     */
    public Planner.PhysicalPlan explainQuery(String sql) 
            throws QueryExceptions.ParseException, QueryExceptions.PlanningException {
        return explainQuery(new QueryApi.QueryRequest(sql));
    }
    
    /**
     * Get query execution plan without executing.
     */
    public Planner.PhysicalPlan explainQuery(QueryApi.QueryRequest request) 
            throws QueryExceptions.ParseException, QueryExceptions.PlanningException {
        
        if (request == null || request.sql == null || request.sql.trim().isEmpty()) {
            throw new QueryExceptions.ParseException("Empty or null SQL query");
        }
        
        // Parse and plan, but don't execute
        Ast.Statement statement = parseSql(request.sql);
        return planQuery(statement, request);
    }
    
    /**
     * Get storage statistics (useful for monitoring).
     */
    public java.util.Map<String, Object> getStorageStats() {
        return store.stats();
    }
    
    /**
     * Get list of available tables.
     */
    public java.util.List<String> getTableNames() {
        return store.getTableNames();
    }
    
    /**
     * Execute multiple queries in sequence and return all results.
     * Useful for batch processing.
     */
    public java.util.List<QueryApi.QueryResult> executeQueries(java.util.List<QueryApi.QueryRequest> requests) {
        java.util.List<QueryApi.QueryResult> results = new java.util.ArrayList<>();
        
        for (QueryApi.QueryRequest request : requests) {
            try {
                QueryApi.QueryResult result = executeQuery(request);
                results.add(result);
            } catch (Exception e) {
                // Create error result
                String errorMessage = "Query failed: " + e.getMessage();
                QueryApi.QueryResult errorResult = new QueryApi.QueryResult(
                    java.util.Arrays.asList("error"),
                    java.util.Arrays.asList(java.util.Arrays.asList(errorMessage)),
                    0, 0, 0
                );
                results.add(errorResult);
            }
        }
        
        return results;
    }
    
    /**
     * Execute multiple SQL strings in sequence.
     */
    public java.util.List<QueryApi.QueryResult> executeSqlQueries(java.util.List<String> sqlQueries) {
        java.util.List<QueryApi.QueryRequest> requests = sqlQueries.stream()
            .map(QueryApi.QueryRequest::new)
            .collect(java.util.stream.Collectors.toList());
        
        return executeQueries(requests);
    }
    
    /**
     * Check if a table exists.
     */
    public boolean tableExists(String tableName) {
        return store.getTableNames().contains(tableName);
    }
    
    /**
     * Get the total number of events across all tables.
     */
    public long getTotalEventCount() {
        return store.size();
    }
}