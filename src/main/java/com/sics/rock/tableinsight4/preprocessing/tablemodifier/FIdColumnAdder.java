package com.sics.rock.tableinsight4.preprocessing.tablemodifier;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.column.FColumnInfoFactory;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Random;

/**
 * id-column appender
 * (appender into origin table)
 *
 * @author zhaorx
 */
public class FIdColumnAdder {

    private static final Logger logger = LoggerFactory.getLogger(FIdColumnAdder.class);

    private final String idColumnName;

    public FIdColumnAdder(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    /**
     * Add an unique id column to the table.
     * Note that it mey be not start from 0 and consecutively.
     */
    public Dataset<Row> addOnDatasetIfAbsent(Dataset<Row> table) {

        for (String column : table.columns()) {
            if (idColumnName.equals(column)) return table;
        }

        if (logger.isDebugEnabled()) {
            return table.withColumn(idColumnName, functions.monotonically_increasing_id().plus(((long) Integer.MAX_VALUE) * new Random().nextInt(100000)));
        } else {
            return table.withColumn(idColumnName, functions.monotonically_increasing_id());
        }
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
