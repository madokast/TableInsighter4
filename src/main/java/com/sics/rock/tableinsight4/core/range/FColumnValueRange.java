package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.table.column.FColumns;

import java.util.List;

/**
 * @author zhaorx
 */
public class FColumnValueRange {

    private final String tableName;

    private final String columnName;

    private final List<FRange> ranges;

    public FColumnValueRange(String tableName, String columnName, List<FRange> ranges) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.ranges = ranges;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public List<FRange> getRanges() {
        return ranges;
    }

    public String identifier() {
        return FColumns.columnIdentifier(tableName, columnName);
    }
}
