package smartquery.query;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.util.*;

/**
 * Abstract Syntax Tree node definitions for SQL queries.
 * These classes represent the parsed structure of SQL statements.
 */
public class Ast {
    
    // Base interfaces
    public interface Statement {}
    
    public interface Expr {}
    
    public interface SelectItem {}
    
    // Main statement implementation
    public static class Select implements Statement {
        public final List<SelectItem> items;
        public final String table;
        public final Expr where;
        public final List<String> groupBy;
        public final List<OrderItem> orderBy;
        public final Integer limit;
        
        public Select(List<SelectItem> items, String table, Expr where, 
                     List<String> groupBy, List<OrderItem> orderBy, Integer limit) {
            this.items = items != null ? items : new ArrayList<>();
            this.table = table;
            this.where = where;
            this.groupBy = groupBy != null ? groupBy : new ArrayList<>();
            this.orderBy = orderBy != null ? orderBy : new ArrayList<>();
            this.limit = limit;
        }
        
        @Override
        public String toString() {
            return String.format("Select{items=%s, table=%s, where=%s, groupBy=%s, orderBy=%s, limit=%s}",
                items, table, where, groupBy, orderBy, limit);
        }
    }
    
    // Select item implementations
    public static class Star implements SelectItem {
        @Override
        public String toString() {
            return "*";
        }
    }
    
    public static class Col implements SelectItem {
        public final String column;
        public final String alias;
        
        public Col(String column, String alias) {
            this.column = column;
            this.alias = alias;
        }
        
        @Override
        public String toString() {
            return alias != null ? column + " AS " + alias : column;
        }
    }
    
    public static class Agg implements SelectItem {
        public final AggFunction function;
        public final String column; // null for COUNT(*)
        public final String alias;
        
        public Agg(AggFunction function, String column, String alias) {
            this.function = function;
            this.column = column;
            this.alias = alias;
        }
        
        @Override
        public String toString() {
            String func = function + "(" + (column != null ? column : "*") + ")";
            return alias != null ? func + " AS " + alias : func;
        }
    }
    
    // Aggregate function types
    public enum AggFunction {
        COUNT_ALL, COUNT, SUM, AVG, MIN, MAX
    }
    
    // Order by item
    public static class OrderItem {
        public final String column;
        public final boolean asc;
        
        public OrderItem(String column, boolean asc) {
            this.column = column;
            this.asc = asc;
        }
        
        @Override
        public String toString() {
            return column + (asc ? " ASC" : " DESC");
        }
    }
    
    // Expression implementations
    public static class Bin implements Expr {
        public final BinOp op;
        public final Expr left;
        public final Expr right;
        
        public Bin(BinOp op, Expr left, Expr right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }
        
        @Override
        public String toString() {
            return "(" + left + " " + op + " " + right + ")";
        }
    }
    
    public enum BinOp {
        AND, OR
    }
    
    public static class Comp implements Expr {
        public final CompOp op;
        public final Expr left;
        public final Expr right;
        
        public Comp(CompOp op, Expr left, Expr right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }
        
        @Override
        public String toString() {
            return "(" + left + " " + op + " " + right + ")";
        }
    }
    
    public enum CompOp {
        EQ("="), NE("!="), LT("<"), LE("<="), GT(">"), GE(">=");
        
        private final String symbol;
        
        CompOp(String symbol) {
            this.symbol = symbol;
        }
        
        @Override
        public String toString() {
            return symbol;
        }
    }
    
    public static class InList implements Expr {
        public final Expr expr;
        public final List<Expr> values;
        
        public InList(Expr expr, List<Expr> values) {
            this.expr = expr;
            this.values = values != null ? values : new ArrayList<>();
        }
        
        @Override
        public String toString() {
            return expr + " IN (" + String.join(", ", values.stream().map(Object::toString).toArray(String[]::new)) + ")";
        }
    }
    
    public static class Between implements Expr {
        public final Expr expr;
        public final Expr low;
        public final Expr high;
        
        public Between(Expr expr, Expr low, Expr high) {
            this.expr = expr;
            this.low = low;
            this.high = high;
        }
        
        @Override
        public String toString() {
            return expr + " BETWEEN " + low + " AND " + high;
        }
    }
    
    public static class LikePrefix implements Expr {
        public final Expr expr;
        public final String prefix;
        
        public LikePrefix(Expr expr, String prefix) {
            this.expr = expr;
            this.prefix = prefix;
        }
        
        @Override
        public String toString() {
            return expr + " LIKE '" + prefix + "%'";
        }
    }
    
    public static class ColRef implements Expr {
        public final String column;
        
        public ColRef(String column) {
            this.column = column;
        }
        
        @Override
        public String toString() {
            return column;
        }
    }
    
    public static class Lit implements Expr {
        public final Object value;
        public final LitType type;
        
        public Lit(Object value, LitType type) {
            this.value = value;
            this.type = type;
        }
        
        @Override
        public String toString() {
            if (type == LitType.STRING) {
                return "'" + value + "'";
            }
            return String.valueOf(value);
        }
    }
    
    public enum LitType {
        INTEGER, FLOAT, STRING
    }
    
    public static class Paren implements Expr {
        public final Expr expr;
        
        public Paren(Expr expr) {
            this.expr = expr;
        }
        
        @Override
        public String toString() {
            return "(" + expr + ")";
        }
    }
}

/**
 * SQL parsing utility using ANTLR4.
 */
class Sql {
    
    /**
     * Parse a SQL string into an AST.
     */
    public static Ast.Statement parse(String sql) {
        try {
            // Create ANTLR input stream
            ANTLRInputStream input = new ANTLRInputStream(sql);
            
            // Create lexer
            SqlParserLexer lexer = new SqlParserLexer(input);
            
            // Create token stream
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            
            // Create parser
            SqlParserParser parser = new SqlParserParser(tokens);
            
            // Set error handling
            parser.removeErrorListeners();
            parser.addErrorListener(new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, 
                                      int line, int charPositionInLine, String msg, RecognitionException e) {
                    throw new RuntimeException("Parse error at line " + line + ":" + charPositionInLine + " - " + msg);
                }
            });
            
            // Parse the statement
            SqlParserParser.StatementContext tree = parser.statement();
            
            // Convert to AST
            return (Ast.Statement) new AstBuilder().visit(tree);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse SQL: " + sql, e);
        }
    }
    
    /**
     * ANTLR visitor to build AST from parse tree.
     */
    private static class AstBuilder extends SqlParserBaseVisitor<Object> {
        
        @Override
        public Ast.Statement visitStatement(SqlParserParser.StatementContext ctx) {
            return (Ast.Statement) visit(ctx.selectStatement());
        }
        
        @Override
        public Ast.Select visitSelectStatement(SqlParserParser.SelectStatementContext ctx) {
            List<Ast.SelectItem> items = (List<Ast.SelectItem>) visit(ctx.selectList());
            String table = ctx.tableName().getText();
            Ast.Expr where = ctx.whereClause() != null ? (Ast.Expr) visit(ctx.whereClause()) : null;
            List<String> groupBy = ctx.groupByList() != null ? (List<String>) visit(ctx.groupByList()) : null;
            List<Ast.OrderItem> orderBy = ctx.orderByList() != null ? (List<Ast.OrderItem>) visit(ctx.orderByList()) : null;
            Integer limit = ctx.limitClause() != null ? (Integer) visit(ctx.limitClause()) : null;
            
            return new Ast.Select(items, table, where, groupBy, orderBy, limit);
        }
        
        @Override
        public List<Ast.SelectItem> visitSelectList(SqlParserParser.SelectListContext ctx) {
            List<Ast.SelectItem> items = new ArrayList<>();
            for (SqlParserParser.SelectItemContext itemCtx : ctx.selectItem()) {
                items.add((Ast.SelectItem) visit(itemCtx));
            }
            return items;
        }
        
        @Override
        public Ast.SelectItem visitSelectStar(SqlParserParser.SelectStarContext ctx) {
            return new Ast.Star();
        }
        
        @Override
        public Ast.SelectItem visitSelectExpression(SqlParserParser.SelectExpressionContext ctx) {
            Ast.Expr expr = (Ast.Expr) visit(ctx.expression());
            String alias = ctx.alias() != null ? ctx.alias().getText() : null;
            
            if (expr instanceof Ast.ColRef) {
                return new Ast.Col(((Ast.ColRef) expr).column, alias);
            } else if (expr instanceof AggregateExpr) {
                AggregateExpr aggExpr = (AggregateExpr) expr;
                return new Ast.Agg(aggExpr.function, aggExpr.column, alias);
            } else {
                // For other expressions, treat as column reference
                return new Ast.Col(expr.toString(), alias);
            }
        }
        
        // Expression visitors
        @Override
        public Ast.Expr visitColumnReference(SqlParserParser.ColumnReferenceContext ctx) {
            return new Ast.ColRef(ctx.identifier().getText());
        }
        
        @Override
        public Ast.Expr visitLiteralExpression(SqlParserParser.LiteralExpressionContext ctx) {
            return (Ast.Expr) visit(ctx.literal());
        }
        
        @Override
        public Ast.Expr visitLiteral(SqlParserParser.LiteralContext ctx) {
            if (ctx.INTEGER() != null) {
                return new Ast.Lit(Long.parseLong(ctx.INTEGER().getText()), Ast.LitType.INTEGER);
            } else if (ctx.FLOAT() != null) {
                return new Ast.Lit(Double.parseDouble(ctx.FLOAT().getText()), Ast.LitType.FLOAT);
            } else if (ctx.STRING() != null) {
                String text = ctx.STRING().getText();
                // Remove surrounding quotes and handle escaped quotes
                String value = text.substring(1, text.length() - 1).replace("''", "'");
                return new Ast.Lit(value, Ast.LitType.STRING);
            }
            throw new RuntimeException("Unknown literal type");
        }
        
        @Override
        public Ast.Expr visitComparisonExpression(SqlParserParser.ComparisonExpressionContext ctx) {
            Ast.Expr left = (Ast.Expr) visit(ctx.expression(0));
            Ast.Expr right = (Ast.Expr) visit(ctx.expression(1));
            
            Ast.CompOp op;
            switch (ctx.operator.getType()) {
                case SqlParserParser.EQ: op = Ast.CompOp.EQ; break;
                case SqlParserParser.NE: op = Ast.CompOp.NE; break;
                case SqlParserParser.LT: op = Ast.CompOp.LT; break;
                case SqlParserParser.LE: op = Ast.CompOp.LE; break;
                case SqlParserParser.GT: op = Ast.CompOp.GT; break;
                case SqlParserParser.GE: op = Ast.CompOp.GE; break;
                default: throw new RuntimeException("Unknown comparison operator");
            }
            
            return new Ast.Comp(op, left, right);
        }
        
        @Override
        public Ast.Expr visitAndExpression(SqlParserParser.AndExpressionContext ctx) {
            Ast.Expr left = (Ast.Expr) visit(ctx.expression(0));
            Ast.Expr right = (Ast.Expr) visit(ctx.expression(1));
            return new Ast.Bin(Ast.BinOp.AND, left, right);
        }
        
        @Override
        public Ast.Expr visitOrExpression(SqlParserParser.OrExpressionContext ctx) {
            Ast.Expr left = (Ast.Expr) visit(ctx.expression(0));
            Ast.Expr right = (Ast.Expr) visit(ctx.expression(1));
            return new Ast.Bin(Ast.BinOp.OR, left, right);
        }
        
        @Override
        public Ast.Expr visitInExpression(SqlParserParser.InExpressionContext ctx) {
            Ast.Expr expr = (Ast.Expr) visit(ctx.expression());
            List<Ast.Expr> values = (List<Ast.Expr>) visit(ctx.expressionList());
            return new Ast.InList(expr, values);
        }
        
        @Override
        public Ast.Expr visitBetweenExpression(SqlParserParser.BetweenExpressionContext ctx) {
            Ast.Expr expr = (Ast.Expr) visit(ctx.expression(0));
            Ast.Expr low = (Ast.Expr) visit(ctx.expression(1));
            Ast.Expr high = (Ast.Expr) visit(ctx.expression(2));
            return new Ast.Between(expr, low, high);
        }
        
        @Override
        public Ast.Expr visitLikeExpression(SqlParserParser.LikeExpressionContext ctx) {
            Ast.Expr expr = (Ast.Expr) visit(ctx.expression());
            String pattern = ctx.STRING().getText();
            // Remove surrounding quotes
            pattern = pattern.substring(1, pattern.length() - 1);
            
            // Check if it's a prefix pattern (ends with %)
            if (pattern.endsWith("%") && !pattern.substring(0, pattern.length() - 1).contains("%")) {
                String prefix = pattern.substring(0, pattern.length() - 1);
                return new Ast.LikePrefix(expr, prefix);
            } else {
                throw new RuntimeException("Only prefix LIKE patterns (ending with %) are supported");
            }
        }
        
        @Override
        public Ast.Expr visitParenthesizedExpression(SqlParserParser.ParenthesizedExpressionContext ctx) {
            return new Ast.Paren((Ast.Expr) visit(ctx.expression()));
        }
        
        @Override
        public List<Ast.Expr> visitExpressionList(SqlParserParser.ExpressionListContext ctx) {
            List<Ast.Expr> expressions = new ArrayList<>();
            for (SqlParserParser.ExpressionContext exprCtx : ctx.expression()) {
                expressions.add((Ast.Expr) visit(exprCtx));
            }
            return expressions;
        }
        
        // Aggregate function visitors
        @Override
        public AggregateExpr visitAggregateExpression(SqlParserParser.AggregateExpressionContext ctx) {
            return (AggregateExpr) visit(ctx.aggregateFunction());
        }
        
        @Override
        public AggregateExpr visitCountStarFunction(SqlParserParser.CountStarFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.COUNT_ALL, null);
        }
        
        @Override
        public AggregateExpr visitCountFunction(SqlParserParser.CountFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.COUNT, ctx.identifier().getText());
        }
        
        @Override
        public AggregateExpr visitSumFunction(SqlParserParser.SumFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.SUM, ctx.identifier().getText());
        }
        
        @Override
        public AggregateExpr visitAvgFunction(SqlParserParser.AvgFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.AVG, ctx.identifier().getText());
        }
        
        @Override
        public AggregateExpr visitMinFunction(SqlParserParser.MinFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.MIN, ctx.identifier().getText());
        }
        
        @Override
        public AggregateExpr visitMaxFunction(SqlParserParser.MaxFunctionContext ctx) {
            return new AggregateExpr(Ast.AggFunction.MAX, ctx.identifier().getText());
        }
        
        // Other visitors
        @Override
        public Ast.Expr visitWhereClause(SqlParserParser.WhereClauseContext ctx) {
            return (Ast.Expr) visit(ctx.expression());
        }
        
        @Override
        public List<String> visitGroupByList(SqlParserParser.GroupByListContext ctx) {
            List<String> columns = new ArrayList<>();
            for (SqlParserParser.IdentifierContext idCtx : ctx.identifier()) {
                columns.add(idCtx.getText());
            }
            return columns;
        }
        
        @Override
        public List<Ast.OrderItem> visitOrderByList(SqlParserParser.OrderByListContext ctx) {
            List<Ast.OrderItem> items = new ArrayList<>();
            for (SqlParserParser.OrderByItemContext itemCtx : ctx.orderByItem()) {
                items.add((Ast.OrderItem) visit(itemCtx));
            }
            return items;
        }
        
        @Override
        public Ast.OrderItem visitOrderByItem(SqlParserParser.OrderByItemContext ctx) {
            String column = ctx.identifier().getText();
            boolean asc = ctx.DESC() == null; // Default to ASC if not specified
            return new Ast.OrderItem(column, asc);
        }
        
        @Override
        public Integer visitLimitClause(SqlParserParser.LimitClauseContext ctx) {
            return Integer.parseInt(ctx.INTEGER().getText());
        }
    }
    
    /**
     * Helper class for aggregate expressions during parsing.
     */
    private static class AggregateExpr implements Ast.Expr {
        public final Ast.AggFunction function;
        public final String column;
        
        public AggregateExpr(Ast.AggFunction function, String column) {
            this.function = function;
            this.column = column;
        }
        
        @Override
        public String toString() {
            return function + "(" + (column != null ? column : "*") + ")";
        }
    }
}