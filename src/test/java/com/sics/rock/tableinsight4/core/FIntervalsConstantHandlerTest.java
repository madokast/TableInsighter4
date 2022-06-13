package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;

public class FIntervalsConstantHandlerTest extends FTableInsightEnv {

    @Test
    public void search() {

        final FTableInfo tableInfo = FExamples.doubleNumberNullColumn7();

        tableInfo.getColumns().forEach(c -> {

            final FIntervalConstantConfig usingDefaultConfig = FIntervalConstantConfig.findIntervalConstant();
            usingDefaultConfig.addExternalIntervalConstant("10");
            c.setIntervalConstantInfo(usingDefaultConfig);
        });

        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(tableInfo));
        final FIntervalsConstantHandler handler = new FIntervalsConstantHandler();
        handler.generateIntervalConstant(tableDatasetMap);
    }
}