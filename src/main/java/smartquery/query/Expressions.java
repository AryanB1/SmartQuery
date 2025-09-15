package smartquery.query;

import smartquery.storage.ColumnStore;
import java.util.*;
import java.util.function.Predicate;

/**
 * Expression evaluation and predicate building for query execution.
 * Handles column resolution, type coercion, and predicate construction from AST expressions.
 */
public class Expressions {
    
    /**
     * Build a row predicate from an AST expression.
     * This converts the WHERE clause AST into a function that can test rows.
     */
    public static Predicate<ColumnStore.Row> buildRowPredicate(Ast.Expr expr) {
        if (expr == null) {
            return row -> true; // No filter
        }
        
        return new ExpressionEvaluator().buildPredicate(expr);
    }
    
    /**
     * Get the value of a column from a row.
     * Handles base columns (ts, table, userId, event) and properties.
     */
    public static Object getValue(ColumnStore.Row row, String column) {
        // Handle case-insensitive column lookup
        String lowerColumn = column.toLowerCase();
        
        switch (lowerColumn) {
            case "ts":
            case "timestamp":
                return row.getTimestamp();
            case "table":
                return row.getTable();
            case "userid":
            case "user_id":
                return row.getUserId();
            case "event":
                return row.getEvent();
            default:
                // Look in properties
                return row.getProperty(column);
        }
    }
    
    /**
     * Check if a string starts with a prefix (for LIKE prefix% patterns).
     */
    public static boolean likePrefix(String text, String prefix) {
        if (text == null || prefix == null) {
            return false;
        }
        return text.toLowerCase().startsWith(prefix.toLowerCase());
    }
    
    /**
     * Compare two values with type coercion.
     * If both are numeric, compare numerically; otherwise compare as strings.
     */
    public static int compareValues(Object left, Object right) {
        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;
        
        // Try numeric comparison first
        Double leftNum = toNumber(left);
        Double rightNum = toNumber(right);
        
        if (leftNum != null && rightNum != null) {
            return Double.compare(leftNum, rightNum);
        }
        
        // Fall back to string comparison
        return left.toString().compareToIgnoreCase(right.toString());
    }
    
    /**
     * Convert a value to a number if possible.
     */
    public static Double toNumber(Object value) {
        if (value == null) return null;
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        
        return null;
    }
    
    /**
     * Check if two values are equal with type coercion.
     */
    public static boolean valuesEqual(Object left, Object right) {
        return compareValues(left, right) == 0;
    }
    
    /**
     * Expression evaluator that builds predicates from AST expressions.
     */
    private static class ExpressionEvaluator {
        
        public Predicate<ColumnStore.Row> buildPredicate(Ast.Expr expr) {
            if (expr instanceof Ast.Bin) {
                return buildBinaryPredicate((Ast.Bin) expr);
            } else if (expr instanceof Ast.Comp) {
                return buildComparisonPredicate((Ast.Comp) expr);
            } else if (expr instanceof Ast.InList) {
                return buildInPredicate((Ast.InList) expr);
            } else if (expr instanceof Ast.Between) {
                return buildBetweenPredicate((Ast.Between) expr);
            } else if (expr instanceof Ast.LikePrefix) {
                return buildLikePredicate((Ast.LikePrefix) expr);
            } else if (expr instanceof Ast.Paren) {
                return buildPredicate(((Ast.Paren) expr).expr);
            } else {
                throw new RuntimeException("Unsupported expression type: " + expr.getClass());
            }
        }
        
        private Predicate<ColumnStore.Row> buildBinaryPredicate(Ast.Bin bin) {
            Predicate<ColumnStore.Row> leftPred = buildPredicate(bin.left);
            Predicate<ColumnStore.Row> rightPred = buildPredicate(bin.right);
            
            switch (bin.op) {
                case AND:
                    return row -> leftPred.test(row) && rightPred.test(row);
                case OR:
                    return row -> leftPred.test(row) || rightPred.test(row);
                default:
                    throw new RuntimeException("Unsupported binary operator: " + bin.op);
            }
        }
        
        private Predicate<ColumnStore.Row> buildComparisonPredicate(Ast.Comp comp) {
            return row -> {
                Object leftVal = evaluateExpression(comp.left, row);
                Object rightVal = evaluateExpression(comp.right, row);
                
                int cmp = compareValues(leftVal, rightVal);
                
                switch (comp.op) {
                    case EQ:
                        return cmp == 0;
                    case NE:
                        return cmp != 0;
                    case LT:
                        return cmp < 0;
                    case LE:
                        return cmp <= 0;
                    case GT:
                        return cmp > 0;
                    case GE:
                        return cmp >= 0;
                    default:
                        throw new RuntimeException("Unsupported comparison operator: " + comp.op);
                }
            };
        }
        
        private Predicate<ColumnStore.Row> buildInPredicate(Ast.InList inList) {
            return row -> {
                Object exprVal = evaluateExpression(inList.expr, row);
                
                for (Ast.Expr value : inList.values) {
                    Object listVal = evaluateExpression(value, row);
                    if (valuesEqual(exprVal, listVal)) {
                        return true;
                    }
                }
                return false;
            };
        }
        
        private Predicate<ColumnStore.Row> buildBetweenPredicate(Ast.Between between) {
            return row -> {
                Object exprVal = evaluateExpression(between.expr, row);
                Object lowVal = evaluateExpression(between.low, row);
                Object highVal = evaluateExpression(between.high, row);
                
                return compareValues(exprVal, lowVal) >= 0 && 
                       compareValues(exprVal, highVal) <= 0;
            };
        }
        
        private Predicate<ColumnStore.Row> buildLikePredicate(Ast.LikePrefix like) {
            return row -> {
                Object exprVal = evaluateExpression(like.expr, row);
                if (exprVal == null) return false;
                
                return likePrefix(exprVal.toString(), like.prefix);
            };
        }
        
        private Object evaluateExpression(Ast.Expr expr, ColumnStore.Row row) {
            if (expr instanceof Ast.ColRef) {
                return getValue(row, ((Ast.ColRef) expr).column);
            } else if (expr instanceof Ast.Lit) {
                return ((Ast.Lit) expr).value;
            } else if (expr instanceof Ast.Paren) {
                return evaluateExpression(((Ast.Paren) expr).expr, row);
            } else {
                throw new RuntimeException("Cannot evaluate expression as value: " + expr.getClass());
            }
        }
    }
    
    /**
     * Extract time range constraints from an expression.
     * Returns [fromTs, toTs] if found, null otherwise.
     */
    public static TimeRange extractTimeRange(Ast.Expr expr) {
        if (expr == null) return null;
        
        TimeRangeExtractor extractor = new TimeRangeExtractor();
        return extractor.extract(expr);
    }
    
    /**
     * Remove time range constraints from an expression to avoid double-filtering.
     * Returns the expression with time constraints removed.
     */
    public static Ast.Expr removeTimeConstraints(Ast.Expr expr) {
        if (expr == null) return null;
        
        TimeConstraintRemover remover = new TimeConstraintRemover();
        return remover.remove(expr);
    }
    
    /**
     * Time range constraint.
     */
    public static class TimeRange {
        public final long fromTs;
        public final long toTs;
        
        public TimeRange(long fromTs, long toTs) {
            this.fromTs = fromTs;
            this.toTs = toTs;
        }
        
        @Override
        public String toString() {
            return String.format("TimeRange{%d, %d}", fromTs, toTs);
        }
    }
    
    /**
     * Extracts time range constraints from WHERE expressions.
     */
    private static class TimeRangeExtractor {
        
        public TimeRange extract(Ast.Expr expr) {
            if (expr instanceof Ast.Between) {
                return extractFromBetween((Ast.Between) expr);
            } else if (expr instanceof Ast.Comp) {
                return extractFromComparison((Ast.Comp) expr);
            } else if (expr instanceof Ast.Bin) {
                return extractFromBinary((Ast.Bin) expr);
            } else if (expr instanceof Ast.Paren) {
                return extract(((Ast.Paren) expr).expr);
            }
            return null;
        }
        
        private TimeRange extractFromBetween(Ast.Between between) {
            if (isTimestampColumn(between.expr)) {
                Long low = getLiteralValue(between.low);
                Long high = getLiteralValue(between.high);
                if (low != null && high != null) {
                    return new TimeRange(low, high);
                }
            }
            return null;
        }
        
        private TimeRange extractFromComparison(Ast.Comp comp) {
            if (isTimestampColumn(comp.left)) {
                Long value = getLiteralValue(comp.right);
                if (value != null) {
                    switch (comp.op) {
                        case GE:
                            return new TimeRange(value, Long.MAX_VALUE);
                        case GT:
                            return new TimeRange(value + 1, Long.MAX_VALUE);
                        case LE:
                            return new TimeRange(Long.MIN_VALUE, value);
                        case LT:
                            return new TimeRange(Long.MIN_VALUE, value - 1);
                        case EQ:
                            return new TimeRange(value, value);
                    }
                }
            } else if (isTimestampColumn(comp.right)) {
                Long value = getLiteralValue(comp.left);
                if (value != null) {
                    // Flip the comparison
                    switch (comp.op) {
                        case LE:
                            return new TimeRange(value, Long.MAX_VALUE);
                        case LT:
                            return new TimeRange(value + 1, Long.MAX_VALUE);
                        case GE:
                            return new TimeRange(Long.MIN_VALUE, value);
                        case GT:
                            return new TimeRange(Long.MIN_VALUE, value - 1);
                        case EQ:
                            return new TimeRange(value, value);
                    }
                }
            }
            return null;
        }
        
        private TimeRange extractFromBinary(Ast.Bin bin) {
            if (bin.op == Ast.BinOp.AND) {
                // Try to combine ranges from both sides
                TimeRange left = extract(bin.left);
                TimeRange right = extract(bin.right);
                
                if (left != null && right != null) {
                    return new TimeRange(
                        Math.max(left.fromTs, right.fromTs),
                        Math.min(left.toTs, right.toTs)
                    );
                } else if (left != null) {
                    return left;
                } else if (right != null) {
                    return right;
                }
            }
            // For OR, we can't easily combine ranges
            return null;
        }
        
        private boolean isTimestampColumn(Ast.Expr expr) {
            if (expr instanceof Ast.ColRef) {
                String col = ((Ast.ColRef) expr).column.toLowerCase();
                return col.equals("ts") || col.equals("timestamp");
            }
            return false;
        }
        
        private Long getLiteralValue(Ast.Expr expr) {
            if (expr instanceof Ast.Lit) {
                Ast.Lit lit = (Ast.Lit) expr;
                if (lit.type == Ast.LitType.INTEGER) {
                    return ((Number) lit.value).longValue();
                }
            }
            return null;
        }
    }
    
    /**
     * Removes time constraints from expressions to avoid double-filtering.
     */
    private static class TimeConstraintRemover {
        
        public Ast.Expr remove(Ast.Expr expr) {
            if (expr instanceof Ast.Between) {
                if (isTimeConstraint((Ast.Between) expr)) {
                    return null; // Remove this constraint
                }
                return expr;
            } else if (expr instanceof Ast.Comp) {
                if (isTimeConstraint((Ast.Comp) expr)) {
                    return null; // Remove this constraint
                }
                return expr;
            } else if (expr instanceof Ast.Bin) {
                return removeBinary((Ast.Bin) expr);
            } else if (expr instanceof Ast.Paren) {
                Ast.Expr inner = remove(((Ast.Paren) expr).expr);
                return inner != null ? new Ast.Paren(inner) : null;
            }
            return expr;
        }
        
        private Ast.Expr removeBinary(Ast.Bin bin) {
            Ast.Expr left = remove(bin.left);
            Ast.Expr right = remove(bin.right);
            
            if (left == null && right == null) {
                return null;
            } else if (left == null) {
                return right;
            } else if (right == null) {
                return left;
            } else {
                return new Ast.Bin(bin.op, left, right);
            }
        }
        
        private boolean isTimeConstraint(Ast.Between between) {
            return isTimestampColumn(between.expr);
        }
        
        private boolean isTimeConstraint(Ast.Comp comp) {
            return isTimestampColumn(comp.left) || isTimestampColumn(comp.right);
        }
        
        private boolean isTimestampColumn(Ast.Expr expr) {
            if (expr instanceof Ast.ColRef) {
                String col = ((Ast.ColRef) expr).column.toLowerCase();
                return col.equals("ts") || col.equals("timestamp");
            }
            return false;
        }
    }
}