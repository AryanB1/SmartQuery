package smartquery.index;

import smartquery.storage.ColumnStore;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core index types and interfaces for secondary indexing.
 * Provides bitmap indexes for categorical data and B-tree indexes for numeric data.
 */
public class IndexTypes {

    /**
     * Interface for all secondary indexes.
     */
    public interface SecondaryIndex {
        String table();
        String column();
        String segmentId();
        void build(List<ColumnStore.Row> rows);
        IntSet lookupEquals(String value);
        IntSet lookupIn(List<String> values);
        IntSet lookupRange(Double lo, boolean includeLo, Double hi, boolean includeHi);
        long memoryBytes();
        Map<String, Object> stats();
    }

    /**
     * Simple bitmap index for low-to-medium cardinality string columns.
     * Uses BitSet for efficient storage and set operations.
     */
    public static class BitmapIndex implements SecondaryIndex {
        private final String table;
        private final String column;
        private final String segmentId;
        private final Map<String, BitSet> valueToRows = new ConcurrentHashMap<>();
        private int rowCount = 0;

        public BitmapIndex(String table, String column, String segmentId) {
            this.table = table;
            this.column = column;
            this.segmentId = segmentId;
        }

        @Override
        public String table() {
            return table;
        }

        @Override
        public String column() {
            return column;
        }

        @Override
        public String segmentId() {
            return segmentId;
        }

        @Override
        public void build(List<ColumnStore.Row> rows) {
            valueToRows.clear();
            rowCount = rows.size();

            for (int i = 0; i < rows.size(); i++) {
                ColumnStore.Row row = rows.get(i);
                String value = extractStringValue(row, column);
                if (value != null) {
                    valueToRows.computeIfAbsent(value, k -> new BitSet()).set(i);
                }
            }
        }

        @Override
        public IntSet lookupEquals(String value) {
            BitSet bitSet = valueToRows.get(value);
            return bitSet != null ? new BitSetIntSet(bitSet) : IntSet.empty();
        }

        @Override
        public IntSet lookupIn(List<String> values) {
            BitSet result = new BitSet();
            for (String value : values) {
                BitSet bitSet = valueToRows.get(value);
                if (bitSet != null) {
                    result.or(bitSet);
                }
            }
            return new BitSetIntSet(result);
        }

        @Override
        public IntSet lookupRange(Double lo, boolean includeLo, Double hi, boolean includeHi) {
            // Not supported for string columns
            return null;
        }

        @Override
        public long memoryBytes() {
            long total = 0;
            for (Map.Entry<String, BitSet> entry : valueToRows.entrySet()) {
                total += entry.getKey().length() * 2; // UTF-16 string
                total += entry.getValue().size() / 8; // BitSet memory approximation
            }
            return total + 64; // Object overhead
        }

        @Override
        public Map<String, Object> stats() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("type", "bitmap");
            stats.put("table", table);
            stats.put("column", column);
            stats.put("segmentId", segmentId);
            stats.put("distinctValues", valueToRows.size());
            stats.put("rowCount", rowCount);
            stats.put("memoryBytes", memoryBytes());
            return stats;
        }

        private String extractStringValue(ColumnStore.Row row, String column) {
            switch (column) {
                case "ts":
                    return String.valueOf(row.getTimestamp());
                case "table":
                    return row.getTable();
                case "userId":
                    return row.getUserId();
                case "event":
                    return row.getEvent();
                default:
                    if (column.startsWith("props.")) {
                        String propKey = column.substring(6);
                        return row.getProperty(propKey);
                    }
                    return null;
            }
        }
    }

    /**
     * B-tree index for numeric columns and range queries.
     * Uses sorted arrays for simplicity and good cache performance.
     */
    public static class BTreeIndex implements SecondaryIndex {
        private final String table;
        private final String column;
        private final String segmentId;
        private final List<IndexEntry> entries = new ArrayList<>();

        public BTreeIndex(String table, String column, String segmentId) {
            this.table = table;
            this.column = column;
            this.segmentId = segmentId;
        }

        @Override
        public String table() {
            return table;
        }

        @Override
        public String column() {
            return column;
        }

        @Override
        public String segmentId() {
            return segmentId;
        }

        @Override
        public void build(List<ColumnStore.Row> rows) {
            entries.clear();

            for (int i = 0; i < rows.size(); i++) {
                ColumnStore.Row row = rows.get(i);
                Double value = extractNumericValue(row, column);

                if (value != null) {
                    entries.add(new IndexEntry(value, i));
                }
            }

            // Sort by value for efficient range queries
            entries.sort(Comparator.comparing(e -> e.value));
        }

        @Override
        public IntSet lookupEquals(String value) {
            try {
                double numValue = Double.parseDouble(value);
                return lookupRange(numValue, true, numValue, true);
            } catch (NumberFormatException e) {
                return IntSet.empty();
            }
        }

        @Override
        public IntSet lookupIn(List<String> values) {
            IntArraySet result = new IntArraySet();
            for (String value : values) {
                IntSet matches = lookupEquals(value);
                result.addAll(matches);
            }
            return result;
        }

        @Override
        public IntSet lookupRange(Double lo, boolean includeLo, Double hi, boolean includeHi) {
            if (lo == null || hi == null) {
                return IntSet.empty();
            }

            IntArraySet result = new IntArraySet();

            for (IndexEntry entry : entries) {
                boolean includeEntry = false;

                if (includeLo && includeHi) {
                    includeEntry = entry.value >= lo && entry.value <= hi;
                } else if (includeLo) {
                    includeEntry = entry.value >= lo && entry.value < hi;
                } else if (includeHi) {
                    includeEntry = entry.value > lo && entry.value <= hi;
                } else {
                    includeEntry = entry.value > lo && entry.value < hi;
                }

                if (includeEntry) {
                    result.add(entry.rowId);
                }
            }

            return result;
        }

        @Override
        public long memoryBytes() {
            return entries.size() * 16 + 64; // 8 bytes double + 4 bytes int + overhead
        }

        @Override
        public Map<String, Object> stats() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("type", "btree");
            stats.put("table", table);
            stats.put("column", column);
            stats.put("segmentId", segmentId);
            stats.put("entryCount", entries.size());
            stats.put("memoryBytes", memoryBytes());
            return stats;
        }

        private Double extractNumericValue(ColumnStore.Row row, String column) {
            try {
                switch (column) {
                    case "ts":
                        return (double) row.getTimestamp();
                    default:
                        if (column.startsWith("props.")) {
                            String propKey = column.substring(6);
                            String propValue = row.getProperty(propKey);
                            return propValue != null ? Double.parseDouble(propValue) : null;
                        }
                        return null;
                }
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private static class IndexEntry {
            final double value;
            final int rowId;

            IndexEntry(double value, int rowId) {
                this.value = value;
                this.rowId = rowId;
            }
        }
    }

    /**
     * Simple interface for integer sets.
     */
    public interface IntSet extends Iterable<Integer> {
        boolean add(int value);
        boolean contains(int value);
        int size();
        boolean isEmpty();
        void addAll(IntSet other);

        static IntSet empty() {
            return new IntArraySet();
        }
    }

    /**
     * Array-based implementation of IntSet.
     * Good for small to medium sets with frequent iteration.
     */
    public static class IntArraySet implements IntSet {
        private int[] values = new int[8];
        private int size = 0;

        @Override
        public boolean add(int value) {
            if (contains(value)) {
                return false;
            }

            if (size >= values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }

            values[size++] = value;
            return true;
        }

        @Override
        public boolean contains(int value) {
            for (int i = 0; i < size; i++) {
                if (values[i] == value) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean isEmpty() {
            return size == 0;
        }

        @Override
        public void addAll(IntSet other) {
            for (int value : other) {
                add(value);
            }
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < size;
                }

                @Override
                public Integer next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return values[index++];
                }
            };
        }
    }

    /**
     * BitSet-based implementation of IntSet.
     * Memory efficient for sparse sets with high maximum values.
     */
    public static class BitSetIntSet implements IntSet {
        private final BitSet bitSet;

        public BitSetIntSet(BitSet bitSet) {
            this.bitSet = (BitSet) bitSet.clone();
        }

        @Override
        public boolean add(int value) {
            boolean wasSet = bitSet.get(value);
            bitSet.set(value);
            return !wasSet;
        }

        @Override
        public boolean contains(int value) {
            return bitSet.get(value);
        }

        @Override
        public int size() {
            return bitSet.cardinality();
        }

        @Override
        public boolean isEmpty() {
            return bitSet.isEmpty();
        }

        @Override
        public void addAll(IntSet other) {
            for (int value : other) {
                add(value);
            }
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<Integer>() {
                private int nextIndex = bitSet.nextSetBit(0);

                @Override
                public boolean hasNext() {
                    return nextIndex >= 0;
                }

                @Override
                public Integer next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    int current = nextIndex;
                    nextIndex = bitSet.nextSetBit(nextIndex + 1);
                    return current;
                }
            };
        }
    }
}