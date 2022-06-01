package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.table.FTables;

import java.util.List;

/**
 * @author zhaorx
 */
public class FRangeInfo {

    private final String tableName;

    private final String columnName;

    private final List<FPair<Double, Double>> ranges;

    public FRangeInfo(String tableName, String columnName, List<FPair<Double, Double>> ranges) {
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

    public List<FPair<Double, Double>> getRanges() {
        return ranges;
    }

    public String identifier() {
        return FTables.columnIdentifier(tableName, columnName);
    }
}