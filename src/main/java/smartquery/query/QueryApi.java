package smartquery.query;

import java.util.*;

/**
 * Data Transfer Objects for the Query API.
 * These classes define the public interface for submitting queries and receiving results.
 */
public class QueryApi {
    
    /**
     * Request object for executing SQL queries.
     */
    public static class QueryRequest {
        
        /** The SQL query string to execute */
        public final String sql;
        
        /** Optional hint for result set size limit */
        public final Integer limitHint;
        
        /** Whether to use vectorized execution (future feature) */
        public final boolean vectorized;
        
        /**
         * Constructor with all parameters.
         */
        public QueryRequest(String sql, Integer limitHint, boolean vectorized) {
            this.sql = sql;
            this.limitHint = limitHint;
            this.vectorized = vectorized;
        }
        
        /**
         * Constructor with just SQL query.
         */
        public QueryRequest(String sql) {
            this(sql, null, false);
        }
        
        /**
         * Constructor with SQL and limit hint.
         */
        public QueryRequest(String sql, Integer limitHint) {
            this(sql, limitHint, false);
        }
        
        @Override
        public String toString() {
            return String.format("QueryRequest{sql='%s', limitHint=%s, vectorized=%s}",
                sql, limitHint, vectorized);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryRequest that = (QueryRequest) o;
            return vectorized == that.vectorized &&
                   Objects.equals(sql, that.sql) &&
                   Objects.equals(limitHint, that.limitHint);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(sql, limitHint, vectorized);
        }
    }
    
    /**
     * Result object containing query execution results and metadata.
     */
    public static class QueryResult {
        
        /** Column names in the result set */
        public final List<String> columns;
        
        /** Result rows, each row is a list of values corresponding to columns */
        public final List<List<Object>> rows;
        
        /** Number of rows scanned during execution */
        public final long scannedRows;
        
        /** Number of rows that matched the WHERE clause */
        public final long matchedRows;
        
        /** Query execution time in milliseconds */
        public final long elapsedMillis;
        
        /**
         * Constructor with all fields.
         */
        public QueryResult(List<String> columns, List<List<Object>> rows,
                          long scannedRows, long matchedRows, long elapsedMillis) {
            this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
            this.rows = rows != null ? new ArrayList<>(rows) : new ArrayList<>();
            this.scannedRows = scannedRows;
            this.matchedRows = matchedRows;
            this.elapsedMillis = elapsedMillis;
        }
        
        /**
         * Get the number of result rows.
         */
        public int getRowCount() {
            return rows.size();
        }
        
        /**
         * Get the number of columns.
         */
        public int getColumnCount() {
            return columns.size();
        }
        
        /**
         * Check if the result set is empty.
         */
        public boolean isEmpty() {
            return rows.isEmpty();
        }
        
        /**
         * Get a specific cell value.
         */
        public Object getValue(int rowIndex, int columnIndex) {
            if (rowIndex < 0 || rowIndex >= rows.size()) {
                throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
            }
            List<Object> row = rows.get(rowIndex);
            if (columnIndex < 0 || columnIndex >= row.size()) {
                throw new IndexOutOfBoundsException("Column index out of bounds: " + columnIndex);
            }
            return row.get(columnIndex);
        }
        
        /**
         * Get a specific cell value by column name.
         */
        public Object getValue(int rowIndex, String columnName) {
            int columnIndex = columns.indexOf(columnName);
            if (columnIndex == -1) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            return getValue(rowIndex, columnIndex);
        }
        
        /**
         * Get a complete row.
         */
        public List<Object> getRow(int rowIndex) {
            if (rowIndex < 0 || rowIndex >= rows.size()) {
                throw new IndexOutOfBoundsException("Row index out of bounds: " + rowIndex);
            }
            return new ArrayList<>(rows.get(rowIndex));
        }
        
        /**
         * Get query execution statistics as a formatted string.
         */
        public String getStats() {
            return String.format(
                "Scanned: %,d rows, Matched: %,d rows, Results: %,d rows, Time: %,d ms",
                scannedRows, matchedRows, getRowCount(), elapsedMillis
            );
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("QueryResult{columns=%s, rows=%d, stats=[%s]}",
                columns, getRowCount(), getStats()));
            
            if (!isEmpty() && getRowCount() <= 10) {
                sb.append("\nRows:\n");
                for (int i = 0; i < Math.min(getRowCount(), 5); i++) {
                    sb.append("  ").append(getRow(i)).append("\n");
                }
                if (getRowCount() > 5) {
                    sb.append("  ... (").append(getRowCount() - 5).append(" more rows)\n");
                }
            }
            
            return sb.toString();
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryResult that = (QueryResult) o;
            return scannedRows == that.scannedRows &&
                   matchedRows == that.matchedRows &&
                   elapsedMillis == that.elapsedMillis &&
                   Objects.equals(columns, that.columns) &&
                   Objects.equals(rows, that.rows);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(columns, rows, scannedRows, matchedRows, elapsedMillis);
        }
        
        /**
         * Create an empty result with just column names.
         */
        public static QueryResult empty(List<String> columns, long elapsedMillis) {
            return new QueryResult(columns, new ArrayList<>(), 0, 0, elapsedMillis);
        }
        
        /**
         * Create an empty result with no columns.
         */
        public static QueryResult empty(long elapsedMillis) {
            return new QueryResult(new ArrayList<>(), new ArrayList<>(), 0, 0, elapsedMillis);
        }
    }
}