package smartquery.query;

/**
 * Exception classes for the query module.
 * These provide structured error handling for different phases of query processing.
 */
public class QueryExceptions {
    
    /**
     * Exception thrown when SQL parsing fails.
     * This indicates syntax errors or unsupported SQL constructs.
     */
    public static class ParseException extends Exception {
        
        public ParseException(String message) {
            super(message);
        }
        
        public ParseException(String message, Throwable cause) {
            super(message, cause);
        }
        
        /**
         * Create a ParseException for syntax errors.
         */
        public static ParseException syntaxError(String sql, int line, int column, String message) {
            return new ParseException(String.format(
                "Syntax error in SQL at line %d, column %d: %s\nSQL: %s",
                line, column, message, sql
            ));
        }
        
        /**
         * Create a ParseException for unsupported features.
         */
        public static ParseException unsupportedFeature(String feature) {
            return new ParseException("Unsupported SQL feature: " + feature);
        }
    }
    
    /**
     * Exception thrown during query planning phase.
     * This indicates logical errors in the query structure or unsupported operations.
     */
    public static class PlanningException extends Exception {
        
        public PlanningException(String message) {
            super(message);
        }
        
        public PlanningException(String message, Throwable cause) {
            super(message, cause);
        }
        
        /**
         * Create a PlanningException for unsupported query features.
         */
        public static PlanningException unsupportedQuery(String feature) {
            return new PlanningException("Unsupported query feature: " + feature);
        }
        
        /**
         * Create a PlanningException for invalid column references.
         */
        public static PlanningException invalidColumn(String column, String table) {
            return new PlanningException(String.format(
                "Invalid column reference '%s' for table '%s'", column, table
            ));
        }
        
        /**
         * Create a PlanningException for aggregate function errors.
         */
        public static PlanningException invalidAggregate(String function, String reason) {
            return new PlanningException(String.format(
                "Invalid aggregate function %s: %s", function, reason
            ));
        }
        
        /**
         * Create a PlanningException for GROUP BY errors.
         */
        public static PlanningException invalidGroupBy(String reason) {
            return new PlanningException("Invalid GROUP BY clause: " + reason);
        }
    }
    
    /**
     * Exception thrown during query execution.
     * This indicates runtime errors, typically from invalid data or resource issues.
     */
    public static class ExecutionException extends RuntimeException {
        
        public ExecutionException(String message) {
            super(message);
        }
        
        public ExecutionException(String message, Throwable cause) {
            super(message, cause);
        }
        
        /**
         * Create an ExecutionException for data type errors.
         */
        public static ExecutionException typeError(String operation, Object value, String expectedType) {
            return new ExecutionException(String.format(
                "Type error in %s: value '%s' is not of expected type %s",
                operation, value, expectedType
            ));
        }
        
        /**
         * Create an ExecutionException for arithmetic errors.
         */
        public static ExecutionException arithmeticError(String operation, String reason) {
            return new ExecutionException(String.format(
                "Arithmetic error in %s: %s", operation, reason
            ));
        }
        
        /**
         * Create an ExecutionException for resource errors.
         */
        public static ExecutionException resourceError(String resource, String reason) {
            return new ExecutionException(String.format(
                "Resource error with %s: %s", resource, reason
            ));
        }
        
        /**
         * Create an ExecutionException for table access errors.
         */
        public static ExecutionException tableNotFound(String table) {
            return new ExecutionException("Table not found: " + table);
        }
    }
}