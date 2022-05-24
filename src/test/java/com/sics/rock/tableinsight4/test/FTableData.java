package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.table.FTableInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FTableData {

    public FTableInfo tableInfo;

    public Dataset<Row> tableData;

    public FTableData(FTableInfo tableInfo, Dataset<Row> tableData) {
        this.tableInfo = tableInfo;
        this.tableData = tableData;
    }
}
