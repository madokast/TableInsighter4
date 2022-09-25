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


    private final List<FExample> examples = new ArrayList<>();

    public void addExample(FExample example) {
        examples.add(example);
    }

    public int size() {
        return examples.size();
    }

    @Override
    public String toString() {
        return examples.stream().map(FExample::toString).collect(Collectors.joining(",", "[", "]"));
    }


    public static class FExample {
        private final List<FRow> rows;

        private FExample(final List<FRow> rows) {
            this.rows = rows;
        }

        public static FExample of(List<FRow> rows) {
            if (rows.size() == 1) return of(rows.get(0));
            if (rows.size() == 2) return of(rows.get(0), rows.get(1));

            throw new IllegalArgumentException("FExample length should be 1 or 2. rows = " + rows);
        }

        public static FExample of(FRow row) {
            return new FExample(Collections.singletonList(row));
        }

        public static FExample of(FRow leftRow, FRow rightRow) {
            return new FExample(FTiUtils.listOf(leftRow, rightRow));
        }

        public List<FRow> getRows() {
            return rows;
        }

        @Override
        public String toString() {
            return rows.stream().map(FRow::toString).collect(Collectors.joining(",", "[", "]"));
        }
    }

    public static class FRow {
        private final String table;
        private final String rowId;

        private FRow(final String table, final String rowId) {
            this.table = table;
            this.rowId = rowId;
        }

        public static FRow of(String tableName, String rowId) {
            return new FRow(tableName, rowId);
        }

        public String getTable() {
            return table;
        }

        public String getRowId() {
            return rowId;
        }

        @Override
        public String toString() {
            return String.format("{\"table\":\"%s\",\"rowId\":\"%s\"}", table, rowId);
        }
    }

}
