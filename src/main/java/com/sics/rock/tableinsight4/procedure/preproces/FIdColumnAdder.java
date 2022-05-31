package com.sics.rock.tableinsight4.procedure.preproces;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FColumnInfoFactory;
import com.sics.rock.tableinsight4.table.FColumnType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;

public class FIdColumnAdder {

    private final String idColumnName;

    public FIdColumnAdder(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    /**
     * Add an unique id column to the table.
     * Note that it does not start from 0.
     */
    public Dataset<Row> addOnDatasetIfAbsent(Dataset<Row> table) {

        for (String column : table.columns()) {
            if (idColumnName.equals(column)) return table;
        }

        return table.withColumn(idColumnName, functions.monotonically_increasing_id());
    }

    /**
     * Add id column into columnInfos
     */
    public ArrayList<FColumnInfo> addToColumnInfoIfAbsent(ArrayList<FColumnInfo> columns) {
        for (FColumnInfo column : columns) {
            if (column.getColumnName().equals(idColumnName)) {
                column.setColumnType(FColumnType.ID);
                return columns;
            }
        }

        columns.add(0, FColumnInfoFactory.createIdColumn(idColumnName));
        return columns;
    }

}
