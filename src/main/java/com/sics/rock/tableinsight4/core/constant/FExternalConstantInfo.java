package com.sics.rock.tableinsight4.core.constant;

import java.util.List;

/**
 * @author zhaorx
 */
public class FExternalConstantInfo {

    private final String tableName;

    private final String columnName;

    private final List<Object> constants;

    public FExternalConstantInfo(String tableName, String columnName, List<Object> constants) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.constants = constants;
    }
}
