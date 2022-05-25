package com.sics.rock.tableinsight4.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * Meta info of a table
 *
 * @author zhaorx
 */
public class FTableInfo implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FTableInfo.class);

    /**
     * User-defined table name.
     * For output and logging
     */
    private final String tableName;

    /**
     * Inner table name
     */
    private final String innerTableName;

    /**
     * Data path
     */
    private final String tableDataPath;

    /**
     * Length of the table
     * Lazy initialization and -1 as null
     */
    private long length = -1;

    /**
     * Columns of this table.
     * Including derived columns such as range-column and model-column,
     * which are dynamic generated. As the result, the List is modifiable,
     * so we use ArrayList type
     */
    private ArrayList<FColumnInfo> columns = new ArrayList<>();

    public FTableInfo(String tableName, String innerTableName, String tableDataPath) {
        this.tableName = tableName;
        this.innerTableName = innerTableName;
        this.tableDataPath = tableDataPath;
    }

    public void addColumnInfo(FColumnInfo columnInfo) {
        this.columns.add(columnInfo);
    }

    public String getTableName() {
        return tableName;
    }

    public String getInnerTableName() {
        return innerTableName;
    }

    public String getTableDataPath() {
        return tableDataPath;
    }

    public long getLength(Supplier<Long> lengthSupplier) {
        if (length == -1) {
            length = lengthSupplier.get();
        }
        return length;
    }

    public ArrayList<FColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<FColumnInfo> columns) {
        this.columns = columns;
    }
}
