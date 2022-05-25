package com.sics.rock.tableinsight4.processor;

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

    private final SparkSession spark;

    private final String idColumnName;

    public List<FTable> prepareData(List<FTableInfo> tableInfos) {

        return tableInfos.parallelStream().map(tableInfo -> {

            String tableName = tableInfo.getTableName();
            String tableDataPath = tableInfo.getTableDataPath();

            logger.info("Load table {} data from {}", tableName, tableDataPath);
            Dataset<Row> dataset = loadData(tableName, tableDataPath);

            logger.info("Add row_id {} if absent on {}", idColumnName, tableName);
            dataset = FSparkSqlUtils.addRowIdIfAbsent(dataset, idColumnName);
            ArrayList<FColumnInfo> columns = addRowIdColumnIfAbsent(tableInfo.getColumns(), idColumnName);
            tableInfo.setColumns(columns);

            logger.info("Remove columns not found in table {} data", tableName);
            ArrayList<FColumnInfo> filteredColumns = removeNotFoundColumn(tableName, dataset.columns(), tableInfo.getColumns());
            tableInfo.setColumns(filteredColumns);

            logger.info("Type cast in table {}. Only retain columns in request", tableName);
            dataset = FSparkSqlUtils.retainColumnAndCastType(tableName, tableInfo.getInnerTableName(), dataset, filteredColumns);

            return new FTable(tableInfo, dataset);

        }).collect(Collectors.toList());
    }

    private ArrayList<FColumnInfo> addRowIdColumnIfAbsent(ArrayList<FColumnInfo> columns, String idColumnName) {
        for (FColumnInfo column : columns) {
            if (column.getColumnName().equals(idColumnName)) {
                column.setColumnType(FColumnType.ID);
                return columns;
            }
        }

        columns.add(0, FColumnInfoFactory.createIdColumn(idColumnName));
        return columns;
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

    private Dataset<Row> loadData(String tableName, String dataPath) {
        Dataset<Row> tab;
        try {
            if (dataPath.toLowerCase().contains("csv")) {
                tab = spark.read().format("csv")
                        .option("header", true)
                        // Do not infer type. All are string.
                        .option("inferSchema", false)
                        .load(dataPath);
            } else {
                tab = spark.read().orc(dataPath);
            }
        } catch (Throwable t) {
            logger.error("### Can not load table {} from {}. Use empty table.", tableName, dataPath);
            t.printStackTrace();
            tab = spark.emptyDataFrame();
        }
        return tab;
    }


    public FTableDataLoader(SparkSession spark, String idColumnName) {
        this.spark = spark;
        this.idColumnName = idColumnName;

        logger.info("Register spark udf");
        FTypeUtils.registerSparkUDF(spark.sqlContext());
    }
}
