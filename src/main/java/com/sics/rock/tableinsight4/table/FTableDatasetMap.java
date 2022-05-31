package com.sics.rock.tableinsight4.table;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key is inner tableName
 */
public class FTableDatasetMap {

    // Key is external table name
    private Map<String, FTableInfo> tableInfoMap = new ConcurrentHashMap<>();

    // Key is external table name
    private Map<String, Dataset<Row>> datasetMap = new ConcurrentHashMap<>();

    // Key is inner table name
    private Map<String, FTableInfo> innerTableInfoMap = new ConcurrentHashMap<>();

    // Key is inner table name
    private Map<String, Dataset<Row>> innerTableDatasetMap = new ConcurrentHashMap<>();

    public void put(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        final String innerTableName = tableInfo.getInnerTableName();

        tableInfoMap.put(tableName, tableInfo);
        datasetMap.put(tableName, dataset);

        innerTableInfoMap.put(innerTableName, tableInfo);
        innerTableDatasetMap.put(innerTableName, dataset);
    }

    public void updateDataset(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        final String innerTableName = tableInfo.getInnerTableName();

        FAssertUtils.require(() -> tableInfoMap.containsKey(tableName), () -> "TableDatasetMap inconsistency!" + this + " does not contain " + tableName);
        FAssertUtils.require(() -> datasetMap.containsKey(tableName), () -> "TableDatasetMap inconsistency!" + this + " does not contain " + tableName);
        FAssertUtils.require(() -> innerTableInfoMap.containsKey(innerTableName), () -> "TableDatasetMap inconsistency!" + this + " does not contain " + innerTableName);
        FAssertUtils.require(() -> innerTableDatasetMap.containsKey(innerTableName), () -> "TableDatasetMap inconsistency!" + this + " does not contain " + innerTableName);

        datasetMap.put(tableName, dataset);
        innerTableDatasetMap.put(innerTableName, dataset);
    }

    public FTableInfo getTableInfoByTableName(String tableName) {
        return tableInfoMap.get(tableName);
    }

    public FTableInfo getTableInfoByInnerTableName(String innerTableName) {
        return innerTableInfoMap.get(innerTableName);
    }

    public Dataset<Row> getDatasetByTableName(String tableName) {
        return datasetMap.get(tableName);
    }

    public Dataset<Row> getDatasetByInnerTableName(String innerTableName) {
        return innerTableDatasetMap.get(innerTableName);
    }

    @Override
    public String toString() {
        return "FTableDatasetMap{" +
                "tableInfos=" + tableInfoMap.keySet() +
                ", dataset=" + datasetMap.keySet() +
                ", innerTableInfos=" + innerTableInfoMap.keySet() +
                ", innerTableDataset=" + innerTableDatasetMap.keySet() +
                '}';
    }
}
