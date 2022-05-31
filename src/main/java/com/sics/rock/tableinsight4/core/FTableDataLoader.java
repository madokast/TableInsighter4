package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.core.load.FTableLoader;
import com.sics.rock.tableinsight4.core.preproces.FDatasetCastHandler;
import com.sics.rock.tableinsight4.core.preproces.FIdColumnAdder;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
public class FTableDataLoader implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FTableDataLoader.class);

    private final String idColumnName = config().idColumnName;

    private final FTableLoader tableLoader = new FTableLoader();

    private final FIdColumnAdder idColumnAdder = new FIdColumnAdder(idColumnName);

    private final FDatasetCastHandler datasetCastHandler = new FDatasetCastHandler();

    private final Thread environmentOwner = Thread.currentThread();

    public FTableDatasetMap prepareData(List<FTableInfo> tableInfos) {

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();

        tableInfos.parallelStream().forEach(tableInfo -> {
            FTiEnvironment.shareFrom(environmentOwner);

            String tableName = tableInfo.getTableName();
            String tableDataPath = tableInfo.getTableDataPath();

            logger.info("Load table {} data from {}", tableName, tableDataPath);
            Dataset<Row> dataset = tableLoader.load(tableDataPath);

            logger.info("Add row_id {} if absent on {}", idColumnName, tableName);
            dataset = idColumnAdder.addOnDatasetIfAbsent(dataset);
            ArrayList<FColumnInfo> columns = idColumnAdder.addToColumnInfoIfAbsent(tableInfo.getColumns());
            tableInfo.setColumns(columns);

            logger.info("Remove columns not found in table {} data", tableName);
            ArrayList<FColumnInfo> filteredColumns = removeNotFoundColumn(tableName, dataset.columns(), tableInfo.getColumns());
            tableInfo.setColumns(filteredColumns);

            logger.info("Type cast in table {}. Only retain columns in request", tableName);
            dataset = datasetCastHandler.retainColumnAndCastType(tableName, tableInfo.getInnerTableName(), dataset, filteredColumns);

            debug(tableName, dataset);

            tableDatasetMap.put(tableInfo, dataset);

            FTiEnvironment.returnBack(environmentOwner);
        });


        return tableDatasetMap;
    }

    private void debug(String tableName, Dataset<Row> dataset) {
        if (logger.isDebugEnabled()) {
            synchronized (FTableDataLoader.class) {
                logger.debug("Show table {}", tableName);
                dataset.show();
            }
        }
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
}
