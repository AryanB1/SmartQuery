package smartquery.query;

import smartquery.storage.ColumnStore;
import java.util.*;
import java.util.function.Predicate;

/**
 * Query planner that converts AST to physical execution plans.
 * Handles logical to physical planning with optimizations like time range extraction.
 */
public class Planner {
    
    /**
     * Plan a query from AST and request.
     */
    public static PhysicalPlan plan(Ast.Statement statement, QueryApi.QueryRequest request) 
            throws QueryExceptions.PlanningException {
        
        if (!(statement instanceof Ast.Select)) {
            throw QueryExceptions.PlanningException.unsupportedQuery("Only SELECT statements are supported");
        }
        
        Ast.Select select = (Ast.Select) statement;
        return planSelect(select, request);
    }
    
    /**
     * Plan a SELECT statement.
     */
    private static PhysicalPlan planSelect(Ast.Select select, QueryApi.QueryRequest request) 
            throws QueryExceptions.PlanningException {
        
        // Validate the query
        validateSelect(select);
        
        // Extract time range from WHERE clause
        Expressions.TimeRange timeRange = null;
        Ast.Expr residualPredicate = select.where;
        
        if (select.where != null) {
            timeRange = Expressions.extractTimeRange(select.where);
            residualPredicate = Expressions.removeTimeConstraints(select.where);
        }
        
        // Set default time range if not specified
        long fromTs = timeRange != null ? timeRange.fromTs : Long.MIN_VALUE;
        long toTs = timeRange != null ? timeRange.toTs : Long.MAX_VALUE;
        
        // Build row predicate from residual WHERE clause
        Predicate<ColumnStore.Row> predicate = Expressions.buildRowPredicate(residualPredicate);
        
        // Create scan operator
        Scan scan = new Scan(select.table, fromTs, toTs, predicate);
        
        // Check if we need aggregation
        boolean hasAggregates = hasAggregateSelectItems(select.items);
        boolean hasGroupBy = !select.groupBy.isEmpty();
        
        if (hasAggregates && !hasGroupBy) {
            throw QueryExceptions.PlanningException.invalidAggregate(
                "aggregate functions", "Aggregate functions require GROUP BY clause");
        }
        
        if (hasGroupBy && !hasAggregates) {
            throw QueryExceptions.PlanningException.invalidGroupBy(
                "GROUP BY requires aggregate functions in SELECT");
        }
        
        // Build operators
        List<PhysicalOperator> operators = new ArrayList<>();
        operators.add(scan);
        
        // Add aggregate operator if needed
        if (hasAggregates && hasGroupBy) {
            operators.add(new Aggregate(select.groupBy, getAggregateSpecs(select.items)));
        }
        
        // Add projection operator
        operators.add(new Project(getProjectionSpecs(select.items)));
        
        // Add ordering operator if needed
        if (!select.orderBy.isEmpty()) {
            operators.add(new OrderBy(select.orderBy));
        }
        
        // Add limit operator if needed
        Integer limit = select.limit;
        if (request.limitHint != null) {
            limit = limit != null ? Math.min(limit, request.limitHint) : request.limitHint;
        }
        if (limit != null) {
            operators.add(new Limit(limit));
        }
        
        return new PhysicalPlan(operators);
    }
    
    /**
     * Validate a SELECT statement for supported features.
     */
    private static void validateSelect(Ast.Select select) throws QueryExceptions.PlanningException {
        if (select.table == null || select.table.trim().isEmpty()) {
            throw QueryExceptions.PlanningException.unsupportedQuery("Missing table name");
        }
        
        if (select.items.isEmpty()) {
            throw QueryExceptions.PlanningException.unsupportedQuery("Empty SELECT list");
        }
        
        // Check for unsupported features in WHERE clause
        if (select.where != null) {
            validateExpression(select.where);
        }
    }
    
    /**
     * Validate an expression for supported features.
     */
    private static void validateExpression(Ast.Expr expr) throws QueryExceptions.PlanningException {
        // For now, all expression types in our AST are supported
        // This method can be extended to check for unsupported patterns
        if (expr instanceof Ast.Bin) {
            Ast.Bin bin = (Ast.Bin) expr;
            validateExpression(bin.left);
            validateExpression(bin.right);
        } else if (expr instanceof Ast.Comp) {
            Ast.Comp comp = (Ast.Comp) expr;
            validateExpression(comp.left);
            validateExpression(comp.right);
        } else if (expr instanceof Ast.InList) {
            Ast.InList inList = (Ast.InList) expr;
            validateExpression(inList.expr);
            for (Ast.Expr value : inList.values) {
                validateExpression(value);
            }
        } else if (expr instanceof Ast.Between) {
            Ast.Between between = (Ast.Between) expr;
            validateExpression(between.expr);
            validateExpression(between.low);
            validateExpression(between.high);
        } else if (expr instanceof Ast.LikePrefix) {
            Ast.LikePrefix like = (Ast.LikePrefix) expr;
            validateExpression(like.expr);
        } else if (expr instanceof Ast.Paren) {
            validateExpression(((Ast.Paren) expr).expr);
        }
    }
    
    /**
     * Check if SELECT items contain aggregates.
     */
    private static boolean hasAggregateSelectItems(List<Ast.SelectItem> items) {
        for (Ast.SelectItem item : items) {
            if (item instanceof Ast.Agg) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get aggregate specifications from SELECT items.
     */
    private static List<AggregateSpec> getAggregateSpecs(List<Ast.SelectItem> items) {
        List<AggregateSpec> specs = new ArrayList<>();
        
        for (Ast.SelectItem item : items) {
            if (item instanceof Ast.Agg) {
                Ast.Agg agg = (Ast.Agg) item;
                String alias = agg.alias != null ? agg.alias : 
                              (agg.function + "(" + (agg.column != null ? agg.column : "*") + ")");
                specs.add(new AggregateSpec(agg.function, agg.column, alias));
            }
        }
        
        return specs;
    }
    
    /**
     * Get projection specifications from SELECT items.
     */
    private static List<ProjectionSpec> getProjectionSpecs(List<Ast.SelectItem> items) {
        List<ProjectionSpec> specs = new ArrayList<>();
        
        for (Ast.SelectItem item : items) {
            if (item instanceof Ast.Star) {
                specs.add(new ProjectionSpec("*", null));
            } else if (item instanceof Ast.Col) {
                Ast.Col col = (Ast.Col) item;
                String alias = col.alias != null ? col.alias : col.column;
                specs.add(new ProjectionSpec(col.column, alias));
            } else if (item instanceof Ast.Agg) {
                Ast.Agg agg = (Ast.Agg) item;
                String alias = agg.alias != null ? agg.alias : 
                              (agg.function + "(" + (agg.column != null ? agg.column : "*") + ")");
                specs.add(new ProjectionSpec(alias, alias)); // Reference the aggregate result
            }
        }
        
        return specs;
    }
    
    /**
     * Physical execution plan.
     */
    public static class PhysicalPlan {
        public final List<PhysicalOperator> operators;
        
        public PhysicalPlan(List<PhysicalOperator> operators) {
            this.operators = new ArrayList<>(operators);
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PhysicalPlan:\n");
            for (int i = 0; i < operators.size(); i++) {
                sb.append("  ").append(i + 1).append(". ").append(operators.get(i)).append("\n");
            }
            return sb.toString();
        }
    }
    
    /**
     * Base class for physical operators.
     */
    public static abstract class PhysicalOperator {
    }
    
    /**
     * Scan operator - reads from storage.
     */
    public static class Scan extends PhysicalOperator {
        public final String table;
        public final long fromTs;
        public final long toTs;
        public final Predicate<ColumnStore.Row> predicate;
        
        public Scan(String table, long fromTs, long toTs, Predicate<ColumnStore.Row> predicate) {
            this.table = table;
            this.fromTs = fromTs;
            this.toTs = toTs;
            this.predicate = predicate;
        }
        
        @Override
        public String toString() {
            return String.format("Scan(table=%s, fromTs=%d, toTs=%d, predicate=%s)",
                table, fromTs, toTs, predicate != null ? "present" : "none");
        }
    }
    
    /**
     * Aggregate operator - groups and aggregates rows.
     */
    public static class Aggregate extends PhysicalOperator {
        public final List<String> groupBy;
        public final List<AggregateSpec> aggregates;
        
        public Aggregate(List<String> groupBy, List<AggregateSpec> aggregates) {
            this.groupBy = new ArrayList<>(groupBy);
            this.aggregates = new ArrayList<>(aggregates);
        }
        
        @Override
        public String toString() {
            return String.format("Aggregate(groupBy=%s, aggregates=%s)", groupBy, aggregates);
        }
    }
    
    /**
     * Project operator - selects and renames columns.
     */
    public static class Project extends PhysicalOperator {
        public final List<ProjectionSpec> projections;
        
        public Project(List<ProjectionSpec> projections) {
            this.projections = new ArrayList<>(projections);
        }
        
        @Override
        public String toString() {
            return String.format("Project(projections=%s)", projections);
        }
    }
    
    /**
     * OrderBy operator - sorts rows.
     */
    public static class OrderBy extends PhysicalOperator {
        public final List<Ast.OrderItem> orderItems;
        
        public OrderBy(List<Ast.OrderItem> orderItems) {
            this.orderItems = new ArrayList<>(orderItems);
        }
        
        @Override
        public String toString() {
            return String.format("OrderBy(items=%s)", orderItems);
        }
    }
    
    /**
     * Limit operator - truncates result set.
     */
    public static class Limit extends PhysicalOperator {
        public final int limit;
        
        public Limit(int limit) {
            this.limit = limit;
        }
        
        @Override
        public String toString() {
            return String.format("Limit(%d)", limit);
        }
    }
    
    /**
     * Specification for aggregation.
     */
    public static class AggregateSpec {
        public final Ast.AggFunction function;
        public final String column; // null for COUNT(*)
        public final String alias;
        
        public AggregateSpec(Ast.AggFunction function, String column, String alias) {
            this.function = function;
            this.column = column;
            this.alias = alias;
        }
        
        @Override
        public String toString() {
            return String.format("%s(%s) AS %s", function, column != null ? column : "*", alias);
        }
    }
    
    /**
     * Specification for projection.
     */
    public static class ProjectionSpec {
        public final String column;
        public final String alias;
        
        public ProjectionSpec(String column, String alias) {
            this.column = column;
            this.alias = alias;
        }
        
        @Override
        public String toString() {
            if (alias != null && !alias.equals(column)) {
                return column + " AS " + alias;
            }
            return column;
        }
    }
}