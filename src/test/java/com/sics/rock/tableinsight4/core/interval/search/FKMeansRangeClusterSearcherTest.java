package com.sics.rock.tableinsight4.core.interval.search;

import com.sics.rock.tableinsight4.core.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.core.FTableDataLoader;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FKMeansRangeClusterSearcherTest extends FTableInsightEnv {

    @Test
    public void search() {

        final FTableInfo tableInfo = FExamples.doubleNumberNullColumn7();

        tableInfo.getColumns().forEach(c -> c.setIntervalConstantInfo(FIntervalConstantConfig.findUsingDefaultConfig()));

        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(tableInfo));
        final FIntervalsConstantHandler handler = new FIntervalsConstantHandler();
        handler.generateIntervalConstant(tableDatasetMap);
    }

    @Test
    public void search2() {
        config().kMeansClusterNumber = 3;
        final FTableInfo tableInfo = FExamples.doubleNumberNullColumn7();
        tableInfo.getColumns().forEach(c -> c.setIntervalConstantInfo(FIntervalConstantConfig.findUsingDefaultConfig()));

        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(tableInfo));
        final FIntervalsConstantHandler handler = new FIntervalsConstantHandler();
        handler.generateIntervalConstant(tableDatasetMap);
    }

    @Test
    public void search3() {
        config().kMeansClusterNumber = 3;
        final FTableInfo tableInfo = FExamples.doubleNumberNullColumn7();
        tableInfo.getColumns().forEach(c -> c.setIntervalConstantInfo(FIntervalConstantConfig.find(
                2, 10, false, false
        )));

        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(tableInfo));
        final FIntervalsConstantHandler handler = new FIntervalsConstantHandler();
        handler.generateIntervalConstant(tableDatasetMap);

        tableInfo.getColumns().stream().map(FColumnInfo::getIntervalConstants)
                .flatMap(List::stream)
                .map(r->r.splitInequalityOf("t0.colXX", 3, false))
                .flatMap(List::stream)
                .forEach(logger::info);

    }
}