package smartquery.query;

import smartquery.storage.ColumnStore;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Query executor that implements physical operators and orchestrates execution.
 * Processes physical plans and returns query results.
 */
public class Executor {
    
    private final ColumnStore store;
    
    public Executor(ColumnStore store) {
        this.store = store;
    }
    
    /**
     * Execute a physical plan and return results.
     */
    public QueryApi.QueryResult execute(Planner.PhysicalPlan plan, QueryApi.QueryRequest request) {
        long startTime = System.currentTimeMillis();
        
        try {
            ExecutionContext context = new ExecutionContext();
            
            // Execute operators in sequence
            for (Planner.PhysicalOperator operator : plan.operators) {
                executeOperator(operator, context);
            }
            
            long elapsedMillis = System.currentTimeMillis() - startTime;
            
            return new QueryApi.QueryResult(
                context.columns,
                context.rows,
                context.scannedRows,
                context.matchedRows,
                elapsedMillis
            );
            
        } catch (Exception e) {
            throw new QueryExceptions.ExecutionException("Query execution failed", e);
        }
    }
    
    /**
     * Execute a single operator.
     */
    private void executeOperator(Planner.PhysicalOperator operator, ExecutionContext context) {
        if (operator instanceof Planner.Scan) {
            executeScan((Planner.Scan) operator, context);
        } else if (operator instanceof Planner.Aggregate) {
            executeAggregate((Planner.Aggregate) operator, context);
        } else if (operator instanceof Planner.Project) {
            executeProject((Planner.Project) operator, context);
        } else if (operator instanceof Planner.OrderBy) {
            executeOrderBy((Planner.OrderBy) operator, context);
        } else if (operator instanceof Planner.Limit) {
            executeLimit((Planner.Limit) operator, context);
        } else {
            throw new QueryExceptions.ExecutionException("Unsupported operator: " + operator.getClass());
        }
    }
    
    /**
     * Execute scan operator.
     */
    private void executeScan(Planner.Scan scan, ExecutionContext context) {
        context.rows.clear(); // Start fresh
        
        Iterable<ColumnStore.Row> scanResult = store.scan(scan.table, scan.fromTs, scan.toTs, null);
        
        for (ColumnStore.Row row : scanResult) {
            context.scannedRows++;
            
            if (scan.predicate == null || scan.predicate.test(row)) {
                context.matchedRows++;
                
                // Convert row to list format
                List<Object> rowData = new ArrayList<>();
                rowData.add(row.getTimestamp());
                rowData.add(row.getTable());
                rowData.add(row.getUserId());
                rowData.add(row.getEvent());
                
                // Add properties as additional columns
                if (row.getProps() != null) {
                    for (Map.Entry<String, String> prop : row.getProps().entrySet()) {
                        rowData.add(prop.getValue());
                    }
                }
                
                context.rows.add(rowData);
                context.rawRows.add(row); // Keep original row for column access
            }
        }
        
        // Set base columns
        context.columns.clear();
        context.columns.addAll(Arrays.asList("ts", "table", "userId", "event"));
    }
    
    /**
     * Execute aggregate operator.
     */
    private void executeAggregate(Planner.Aggregate agg, ExecutionContext context) {
        Map<String, AggregateAccumulator> groups = new HashMap<>();
        
        // Group rows
        for (int i = 0; i < context.rawRows.size(); i++) {
            ColumnStore.Row row = context.rawRows.get(i);
            
            // Build group key
            List<String> keyParts = new ArrayList<>();
            for (String groupCol : agg.groupBy) {
                Object value = Expressions.getValue(row, groupCol);
                keyParts.add(value != null ? value.toString() : "NULL");
            }
            String groupKey = String.join("|", keyParts);
            
            // Get or create accumulator
            AggregateAccumulator accumulator = groups.computeIfAbsent(groupKey, 
                k -> new AggregateAccumulator(agg.aggregates));
            
            // Add row to accumulator
            accumulator.add(row);
        }
        
        // Build result rows
        context.rows.clear();
        context.rawRows.clear();
        
        for (Map.Entry<String, AggregateAccumulator> entry : groups.entrySet()) {
            String groupKey = entry.getKey();
            AggregateAccumulator accumulator = entry.getValue();
            
            List<Object> resultRow = new ArrayList<>();
            
            // Add group by columns
            String[] keyParts = groupKey.split("\\|", -1);
            for (String part : keyParts) {
                resultRow.add("NULL".equals(part) ? null : part);
            }
            
            // Add aggregate results
            for (Object aggResult : accumulator.getResults()) {
                resultRow.add(aggResult);
            }
            
            context.rows.add(resultRow);
        }
        
        // Update columns
        context.columns.clear();
        context.columns.addAll(agg.groupBy);
        for (Planner.AggregateSpec spec : agg.aggregates) {
            context.columns.add(spec.alias);
        }
    }
    
    /**
     * Execute projection operator.
     */
    private void executeProject(Planner.Project project, ExecutionContext context) {
        List<String> newColumns = new ArrayList<>();
        List<List<Object>> newRows = new ArrayList<>();
        
        for (Planner.ProjectionSpec spec : project.projections) {
            if ("*".equals(spec.column)) {
                // Add all current columns
                newColumns.addAll(context.columns);
            } else {
                // Add specific column
                newColumns.add(spec.alias != null ? spec.alias : spec.column);
            }
        }
        
        // Project each row
        for (List<Object> row : context.rows) {
            List<Object> newRow = new ArrayList<>();
            
            for (Planner.ProjectionSpec spec : project.projections) {
                if ("*".equals(spec.column)) {
                    // Add all values from current row
                    newRow.addAll(row);
                } else {
                    // Find column index
                    int columnIndex = context.columns.indexOf(spec.column);
                    if (columnIndex >= 0 && columnIndex < row.size()) {
                        newRow.add(row.get(columnIndex));
                    } else {
                        newRow.add(null); // Column not found
                    }
                }
            }
            
            newRows.add(newRow);
        }
        
        context.columns = newColumns;
        context.rows = newRows;
    }
    
    /**
     * Execute order by operator.
     */
    private void executeOrderBy(Planner.OrderBy orderBy, ExecutionContext context) {
        // Create comparator for sorting
        Comparator<List<Object>> comparator = (row1, row2) -> {
            for (Ast.OrderItem orderItem : orderBy.orderItems) {
                int columnIndex = context.columns.indexOf(orderItem.column);
                if (columnIndex >= 0) {
                    Object val1 = columnIndex < row1.size() ? row1.get(columnIndex) : null;
                    Object val2 = columnIndex < row2.size() ? row2.get(columnIndex) : null;
                    
                    int cmp = Expressions.compareValues(val1, val2);
                    if (cmp != 0) {
                        return orderItem.asc ? cmp : -cmp;
                    }
                }
            }
            return 0;
        };
        
        // Sort rows in place
        context.rows.sort(comparator);
    }
    
    /**
     * Execute limit operator.
     */
    private void executeLimit(Planner.Limit limit, ExecutionContext context) {
        if (context.rows.size() > limit.limit) {
            context.rows = context.rows.subList(0, limit.limit);
        }
    }
    
    /**
     * Execution context to pass state between operators.
     */
    private static class ExecutionContext {
        List<String> columns = new ArrayList<>();
        List<List<Object>> rows = new ArrayList<>();
        List<ColumnStore.Row> rawRows = new ArrayList<>(); // For column access during aggregation
        long scannedRows = 0;
        long matchedRows = 0;
    }
    
    /**
     * Accumulator for aggregate functions.
     */
    private static class AggregateAccumulator {
        private final List<Planner.AggregateSpec> specs;
        private final Map<Planner.AggregateSpec, Object> accumulators = new HashMap<>();
        private int count = 0;
        
        public AggregateAccumulator(List<Planner.AggregateSpec> specs) {
            this.specs = new ArrayList<>(specs);
            
            // Initialize accumulators
            for (Planner.AggregateSpec spec : specs) {
                switch (spec.function) {
                    case COUNT_ALL:
                    case COUNT:
                        accumulators.put(spec, 0L);
                        break;
                    case SUM:
                        accumulators.put(spec, 0.0);
                        break;
                    case AVG:
                        accumulators.put(spec, new AvgAccumulator());
                        break;
                    case MIN:
                        accumulators.put(spec, null);
                        break;
                    case MAX:
                        accumulators.put(spec, null);
                        break;
                }
            }
        }
        
        public void add(ColumnStore.Row row) {
            count++;
            
            for (Planner.AggregateSpec spec : specs) {
                Object currentValue = accumulators.get(spec);
                
                switch (spec.function) {
                    case COUNT_ALL:
                        accumulators.put(spec, ((Long) currentValue) + 1);
                        break;
                        
                    case COUNT:
                        Object colValue = Expressions.getValue(row, spec.column);
                        if (colValue != null) {
                            accumulators.put(spec, ((Long) currentValue) + 1);
                        }
                        break;
                        
                    case SUM:
                        Object sumValue = Expressions.getValue(row, spec.column);
                        Double numValue = Expressions.toNumber(sumValue);
                        if (numValue != null) {
                            accumulators.put(spec, ((Double) currentValue) + numValue);
                        }
                        break;
                        
                    case AVG:
                        Object avgValue = Expressions.getValue(row, spec.column);
                        Double avgNumValue = Expressions.toNumber(avgValue);
                        if (avgNumValue != null) {
                            ((AvgAccumulator) currentValue).add(avgNumValue);
                        }
                        break;
                        
                    case MIN:
                        Object minValue = Expressions.getValue(row, spec.column);
                        if (minValue != null) {
                            if (currentValue == null || Expressions.compareValues(minValue, currentValue) < 0) {
                                accumulators.put(spec, minValue);
                            }
                        }
                        break;
                        
                    case MAX:
                        Object maxValue = Expressions.getValue(row, spec.column);
                        if (maxValue != null) {
                            if (currentValue == null || Expressions.compareValues(maxValue, currentValue) > 0) {
                                accumulators.put(spec, maxValue);
                            }
                        }
                        break;
                }
            }
        }
        
        public List<Object> getResults() {
            List<Object> results = new ArrayList<>();
            
            for (Planner.AggregateSpec spec : specs) {
                Object accumulator = accumulators.get(spec);
                
                switch (spec.function) {
                    case COUNT_ALL:
                    case COUNT:
                    case SUM:
                    case MIN:
                    case MAX:
                        results.add(accumulator);
                        break;
                        
                    case AVG:
                        AvgAccumulator avgAcc = (AvgAccumulator) accumulator;
                        results.add(avgAcc.getAverage());
                        break;
                }
            }
            
            return results;
        }
    }
    
    /**
     * Helper class for AVG accumulation.
     */
    private static class AvgAccumulator {
        private double sum = 0.0;
        private long count = 0;
        
        public void add(double value) {
            sum += value;
            count++;
        }
        
        public Double getAverage() {
            return count > 0 ? sum / count : null;
        }
    }
}