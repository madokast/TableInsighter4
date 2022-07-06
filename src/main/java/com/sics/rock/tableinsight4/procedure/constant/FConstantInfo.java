package com.sics.rock.tableinsight4.procedure.constant;

import com.sics.rock.tableinsight4.table.column.FValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhaorx
 */
public class FConstantInfo {

    private static final Logger logger = LoggerFactory.getLogger(FConstantInfo.class);

    private final String tableName;

    private final String columnName;

    private final FValueType type;

    private final List<FConstant<?>> constants;

    public FConstantInfo(String tableName, String columnName, FValueType type, List<FConstant<?>> constants) {
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

    public List<FConstant<?>> getConstants() {
        return constants;
    }

    @Override
    public String toString() {
        final String constants = this.constants.stream().map(FConstant::toString)
                .map(c -> "'" + c + "'")
                .collect(Collectors.joining(", "));
        return String.format("[%s] %s in %s.%s", type.toString(), constants, tableName, columnName);
    }

    public boolean isEmpty() {
        return constants.isEmpty();
    }
}
