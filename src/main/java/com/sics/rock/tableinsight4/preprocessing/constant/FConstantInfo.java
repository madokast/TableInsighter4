package com.sics.rock.tableinsight4.preprocessing.constant;

import com.sics.rock.tableinsight4.table.column.FValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * constant info
 *
 * @author zhaorx
 */
public class FConstantInfo {

    private static final Logger logger = LoggerFactory.getLogger(FConstantInfo.class);

    private final String tableName;

    private final String columnName;

    private final FValueType type;

    private final FConstant<?> constant;

    public FConstantInfo(String tableName, String columnName, FValueType type, FConstant<?> constant) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.type = type;
        this.constant = constant;
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

    public FConstant<?> getConstant() {
        return constant;
    }

    @Override
    public String toString() {
        return String.format("%s(%s) in %s.%s", constant.toString(), type.toString(), tableName, columnName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FConstantInfo that = (FConstantInfo) o;
        return Objects.equals(tableName, that.tableName) &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(type, that.type) &&
                Objects.equals(constant, that.constant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName, type, constant);
    }
}
