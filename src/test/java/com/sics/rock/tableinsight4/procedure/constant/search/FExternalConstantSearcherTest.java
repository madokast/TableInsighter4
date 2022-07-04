package com.sics.rock.tableinsight4.procedure.constant.search;

import com.sics.rock.tableinsight4.procedure.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FConstantConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.junit.Test;

import java.util.List;

public class FExternalConstantSearcherTest extends FSparkEnv {

    @Test
    public void search() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.STRING);
            final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
            columnInfo.setConstantConfig(constantConfig);
            for (int j = 0; j < 2; j++) {
                constantConfig.addExternalConstants("cons" + i + "_" + j);
            }
            tableInfo.addColumnInfo(columnInfo);
        }

        final FIConstantSearcher searcher = new FExternalConstantSearcher();

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(tableInfo, spark.emptyDataFrame());

        final List<FConstantInfo> constantInfoList = searcher.search(tableDatasetMap);
        for (FConstantInfo constantInfo : constantInfoList) {
            logger.info(constantInfo.toString());
        }

    }

    @Test
    public void search2() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.DOUBLE);
            final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
            columnInfo.setConstantConfig(constantConfig);
            for (int j = 0; j < 2; j++) {
                constantConfig.addExternalConstants(i + "." + j);
            }
            tableInfo.addColumnInfo(columnInfo);
        }

        final FIConstantSearcher searcher = new FExternalConstantSearcher();

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(tableInfo, spark.emptyDataFrame());

        final List<FConstantInfo> constantInfoList = searcher.search(tableDatasetMap);
        for (FConstantInfo constantInfo : constantInfoList) {
            logger.info(constantInfo.toString());
        }

    }

    @Test
    public void search3() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.INTEGER);
            final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
            columnInfo.setConstantConfig(constantConfig);
            for (int j = 0; j < 2; j++) {
                constantConfig.addExternalConstants(i + "" + j);
            }
            tableInfo.addColumnInfo(columnInfo);
        }

        final FIConstantSearcher searcher = new FExternalConstantSearcher();

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(tableInfo, spark.emptyDataFrame());

        final List<FConstantInfo> constantInfoList = searcher.search(tableDatasetMap);
        for (FConstantInfo constantInfo : constantInfoList) {
            logger.info(constantInfo.toString());
        }

    }

    @Test
    public void search4() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.DATE);
            final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
            columnInfo.setConstantConfig(constantConfig);
            for (int j = 0; j < 2; j++) {
                constantConfig.addExternalConstants("2022-06-" + (10 + i + j));
            }
            tableInfo.addColumnInfo(columnInfo);
        }

        final FIConstantSearcher searcher = new FExternalConstantSearcher();

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(tableInfo, spark.emptyDataFrame());

        final List<FConstantInfo> constantInfoList = searcher.search(tableDatasetMap);
        for (FConstantInfo constantInfo : constantInfoList) {
            logger.info(constantInfo.toString());
        }

    }

    @Test
    public void search5() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.STRING);
            final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
            columnInfo.setConstantConfig(constantConfig);
            for (int j = 0; j < 2; j++) {
                constantConfig.addExternalConstants("");
                constantConfig.addExternalConstants(null);
            }
            tableInfo.addColumnInfo(columnInfo);
        }

        final FIConstantSearcher searcher = new FExternalConstantSearcher();

        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(tableInfo, spark.emptyDataFrame());

        final List<FConstantInfo> constantInfoList = searcher.search(tableDatasetMap);
        for (FConstantInfo constantInfo : constantInfoList) {
            logger.info(constantInfo.toString());
        }

    }
}