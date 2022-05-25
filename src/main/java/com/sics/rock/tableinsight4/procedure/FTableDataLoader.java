package com.sics.rock.tableinsight4.procedure;

import com.sics.rock.tableinsight4.procedure.load.FTableLoader;
import com.sics.rock.tableinsight4.procedure.preproces.FIdColumnAdder;
import com.sics.rock.tableinsight4.table.*;
import com.sics.rock.tableinsight4.utils.FSparkSqlUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 1. load table data from path.
 * 2. add row_id if absent.
 * 3. remove columns not found in request (columnInfo)
 * 4. do type cast. Retain columns in request (columnInfo)
 *
 * @author zhaorx
 */
public class FTableDataLoader {

    private static final Logger logger = LoggerFactory.getLogger(FTableDataLoader.class);

    private final String idColumnName;

    public List<FTable> prepareData(List<FTableInfo> tableInfos) {
        return tableInfos.parallelStream().map(tableInfo -> {

            String tableName = tableInfo.getTableName();
            String tableDataPath = tableInfo.getTableDataPath();

            logger.info("Load table {} data from {}", tableName, tableDataPath);
            Dataset<Row> dataset = new FTableLoader().load(tableDataPath);

            logger.info("Add row_id {} if absent on {}", idColumnName, tableName);
            FIdColumnAdder idColumnAdder = new FIdColumnAdder(idColumnName);
            dataset = idColumnAdder.addOnDatasetIfAbsent(dataset);
            ArrayList<FColumnInfo> columns = idColumnAdder.addToColumnInfoIfAbsent(tableInfo.getColumns());
            tableInfo.setColumns(columns);

            logger.info("Remove columns not found in table {} data", tableName);
            ArrayList<FColumnInfo> filteredColumns = removeNotFoundColumn(tableName, dataset.columns(), tableInfo.getColumns());
            tableInfo.setColumns(filteredColumns);

            logger.info("Type cast in table {}. Only retain columns in request", tableName);
            dataset = FSparkSqlUtils.retainColumnAndCastType(tableName, tableInfo.getInnerTableName(), dataset, filteredColumns);

            return new FTable(tableInfo, dataset);

        }).collect(Collectors.toList());
    }


    private ArrayList<FColumnInfo> removeNotFoundColumn(
            String tableName, final String[] dataColumns, final List<FColumnInfo> infoColumns) {
        Set<String> existingColumns = new HashSet<>();
        Collections.addAll(existingColumns, dataColumns);
        List<FColumnInfo> filteredColumns = infoColumns.stream().filter(columnInfo -> {
            String columnName = columnInfo.getColumnName();
            if (existingColumns.contains(columnName)) {
                return true;
            } else {
                logger.warn("In table {}, cannot find column {} in table data.", tableName, columnName);
                return false;
            }
        }).collect(Collectors.toList());

        return (ArrayList<FColumnInfo>) filteredColumns;
    }


    public FTableDataLoader(SparkSession spark, String idColumnName) {
        this.idColumnName = idColumnName;

        logger.info("Register spark udf");
        FTypeUtils.registerSparkUDF(spark.sqlContext());
    }
}
