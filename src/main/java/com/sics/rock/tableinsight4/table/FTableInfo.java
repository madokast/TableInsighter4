package com.sics.rock.tableinsight4.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

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
    private String tableName;

    /**
     * Inner table name
     */
    private String innerTableName;

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

    public FTableInfo(String tableName, String innerTableName) {
        this.tableName = tableName;
        this.innerTableName = innerTableName;
    }

    public void addColumnInfo(FColumnInfo columnInfo) {
        this.columns.add(columnInfo);
    }
}
