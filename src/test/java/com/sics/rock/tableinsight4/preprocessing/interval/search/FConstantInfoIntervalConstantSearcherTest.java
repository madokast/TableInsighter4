package com.sics.rock.tableinsight4.preprocessing.interval.search;

import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FConstantInfoIntervalConstantSearcherTest extends FTableInsightEnv {

    private static final Class<FConstantInfoIntervalConstantSearcher> TARGET = FConstantInfoIntervalConstantSearcher.class;

    static {
        logger.info("Test {}", TARGET);
    }

    @Test
    public void search() {

        config().usingConstantCreateInterval = true;
        config().kMeansIntervalSearch = false;

        final FTableInfo tableInfo = FExamples.doubleNumberNullColumn7();
        for (final FColumnInfo column : tableInfo.getColumns()) {
            column.setIntervalConstantInfo(FIntervalConstantConfig.findIntervalConstant());
        }

        final List<FTableInfo> tableInfos = Collections.singletonList(tableInfo);

        final FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(tableInfos);

        new FConstantHandler().generateConstant(tableDatasetMap);

        new FIntervalsConstantHandler().generateIntervalConstant(tableDatasetMap);

        for (final FTableInfo tab : tableDatasetMap.allTableInfos()) {
            for (final FColumnInfo column : tab.getColumns()) {
                final ArrayList<FConstant<?>> constants = column.getConstants();
                final ArrayList<FInterval> intervals = column.getIntervalConstants();

                logger.info("{} consts {}", column, constants.stream().map(FConstant::getConstant).collect(Collectors.toList()));
                logger.info("{} intervals {}", column, intervals.stream().map(i -> i.inequalityOf("_")).collect(Collectors.toList()));

            }
        }
    }
}