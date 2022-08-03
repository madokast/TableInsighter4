package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstantInfo;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FExternalConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FIConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FRatioConstantSearcher;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FConstantConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class FConstantHandlerTest extends FTableInsightEnv {

    @Test
    public void test() {
        final FTableInfo tableInfo = new FTableInfo("tab", "tab", "/null");
        for (int i = 0; i < 5; i++) {
            final FColumnInfo columnInfo = new FColumnInfo("col" + i, FValueType.STRING);
            final FConstantConfig constantConfig = FConstantConfig.notFindConstantValue();
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

        final FConstantHandler handler = new FConstantHandler();
        handler.generateConstant(tableDatasetMap);
        for (FColumnInfo column : tableInfo.getColumns()) {
            for (FConstant<?> constant : column.getConstants()) {
                logger.info("{} find in {}.{}", constant, tableInfo.getTableName(), column.getColumnName());
            }
        }
    }

    @Test
    public void test2() {
        final FTableInfo relation = FExamples.relation();
        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(relation));
        final FRatioConstantSearcher searcher = new FRatioConstantSearcher(
                config().findNullConstant, config().constantUpperLimitRatio, config().constantDownLimitRatio
        );
        for (FConstantInfo c : searcher.search(tableDatasetMap)) {
            logger.info("Find {}", c);
        }

        final FConstantHandler handler = new FConstantHandler();
        handler.generateConstant(tableDatasetMap);
        for (FColumnInfo column : relation.getColumns()) {
            for (FConstant<?> constant : column.getConstants()) {
                logger.info("{} find in {}.{}", constant, relation.getTableName(), column.getColumnName());
            }
        }
    }

    @Test
    public void test3() {
        final FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            final FConstantConfig constantConfig = column.getConstantConfig();
            constantConfig.addExternalConstants(UUID.randomUUID().toString());
            constantConfig.addExternalConstants(UUID.randomUUID().toString());
            constantConfig.addExternalConstants(UUID.randomUUID().toString());
        }

        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(relation));
        final FRatioConstantSearcher searcher = new FRatioConstantSearcher(
                config().findNullConstant, config().constantUpperLimitRatio, config().constantDownLimitRatio
        );
        for (FConstantInfo c : searcher.search(tableDatasetMap)) {
            logger.info("Find {}", c);
        }

        final FConstantHandler handler = new FConstantHandler();
        handler.generateConstant(tableDatasetMap);
        for (FColumnInfo column : relation.getColumns()) {
            for (FConstant<?> constant : column.getConstants()) {
                logger.info("{} find in {}.{}", constant, relation.getTableName(), column.getColumnName());
            }
        }
    }
}