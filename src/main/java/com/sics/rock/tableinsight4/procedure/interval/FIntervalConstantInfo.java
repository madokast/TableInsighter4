package com.sics.rock.tableinsight4.procedure.interval;

import java.util.List;

/**
 * Interval constants in each column
 *
 * @author zhaorx
 */
public class FIntervalConstantInfo {

    public static final String SOURCE_EXTERNAL = "EXTERNAL";
    public static final String SOURCE_K_MEANS = "K-MEANS";
    public static final String SOURCE_DECISION_TREE = "DECISION_TREE";

    private final String tableName;

    private final String columnName;

    private final List<FInterval> intervals;

    private final String source;

    public FIntervalConstantInfo(String tableName, String columnName, List<FInterval> intervals, String source) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.intervals = intervals;
        this.source = source;
    }

    public static FIntervalConstantInfo externalColumnIntervalConstant(String tableName, String columnName, List<FInterval> intervals) {
        return new FIntervalConstantInfo(tableName, columnName, intervals, SOURCE_EXTERNAL);
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public List<FInterval> getIntervals() {
        return intervals;
    }

    @Override
    public String toString() {
        return String.format("%s.%s interval %s in from %s", tableName, columnName, intervals.toString(), source);
    }
}
