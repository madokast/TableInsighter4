package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.utils.FTiUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * positive negative example recorder
 *
 * @author zhaorx
 */
public class FPositiveNegativeExample {


    private final List<Example> examples = new ArrayList<>();

    public void addExample(Example example) {
        examples.add(example);
    }

    public int size() {
        return examples.size();
    }

    @Override
    public String toString() {
        return examples.stream().map(Example::toString).collect(Collectors.joining(",", "[", "]"));
    }


    public static class Example {
        private final List<Row> rows;

        private Example(final List<Row> rows) {
            this.rows = rows;
        }

        public static Example of(List<Row> rows) {
            if (rows.size() == 1) return of(rows.get(0));
            if (rows.size() == 2) return of(rows.get(0), rows.get(1));

            throw new IllegalArgumentException("Example length should be 1 or 2. rows = " + rows);
        }

        public static Example of(Row row) {
            return new Example(Collections.singletonList(row));
        }

        public static Example of(Row leftRow, Row rightRow) {
            return new Example(FTiUtils.listOf(leftRow, rightRow));
        }

        public List<Row> getRows() {
            return rows;
        }

        @Override
        public String toString() {
            return rows.stream().map(Row::toString).collect(Collectors.joining(",", "[", "]"));
        }
    }

    public static class Row {
        private final String table;
        private final String rowId;

        private Row(final String table, final String rowId) {
            this.table = table;
            this.rowId = rowId;
        }

        public static Row of(String tableName, String rowId) {
            return new Row(tableName, rowId);
        }

        public String getTable() {
            return table;
        }

        public String getRowId() {
            return rowId;
        }

        @Override
        public String toString() {
            return String.format("\"table\":\"%s\",\"rowId\":\"%s\"", table, rowId);
        }
    }

}
