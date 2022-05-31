package com.sics.rock.tableinsight4.core.range;

import java.util.List;

/**
 * @author zhaorx
 */
public class FExternalRangeInfo {

    private final String tableName;

    private final String columnName;

    private final List<Double> boundaries;

    public FExternalRangeInfo(String tableName, String columnName, List<Double> boundaries) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.boundaries = boundaries;
    }
}
