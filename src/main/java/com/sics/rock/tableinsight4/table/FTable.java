package com.sics.rock.tableinsight4.table;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * tableInfo and dataset
 *
 * @author zhaorx
 */
public class FTable {

    private FTableInfo tableInfo;

    private Dataset<Row> dataset;

    public FTable(FTableInfo tableInfo, Dataset<Row> dataset) {
        this.tableInfo = tableInfo;
        this.dataset = dataset;
    }

    public FTableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(FTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public void setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
    }
}
