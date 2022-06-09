package com.sics.rock.tableinsight4.core.constant;

import com.sics.rock.tableinsight4.table.column.FValueType;

import java.util.List;

/**
 *
 * @author zhaorx
 */
public class FExternalConstantInfo {

    private final String tableName;

    private final String columnName;

    private final FValueType type;

    private final List<Object> constants;

    public FExternalConstantInfo(String tableName, String columnName, FValueType type, List<Object> constants) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.type = type;
        this.constants = constants;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public FValueType getType() {
        return type;
    }

    public List<Object> getConstants() {
        return constants;
    }
}
