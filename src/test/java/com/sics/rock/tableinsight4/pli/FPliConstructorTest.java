package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.procedure.FConstantHandler;
import com.sics.rock.tableinsight4.procedure.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.procedure.FTableDataLoader;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;

public class FPliConstructorTest extends FTableInsightEnv {

    @Test
    public void construct() {

        FTableInfo relation = FExamples.relation();

        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        FPliConstructor constructor = new FPliConstructor(
                config().idColumnName,
                2,
                spark
        );

        FPLI PLI = constructor.construct(tableDatasetMap);

    }

    @Test
    public void construct2() {

        FTableInfo relation = FExamples.relation();

        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        FPliConstructor constructor = new FPliConstructor(
                config().idColumnName,
                1000,
                spark
        );

        FPLI PLI = constructor.construct(tableDatasetMap);

    }

    @Test
    public void construct3() {
        FTableInfo doubleNumberNullColumn7 = FExamples.doubleNumberNullColumn7();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(doubleNumberNullColumn7));

        FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName, config().sliceLengthForPLI, spark);
        FPLI PLI = pliConstructor.construct(tableDatasetMap);

    }

    @Test
    public void construct4() {
        FTableInfo doubleNumberNullColumn7 = FExamples.doubleNumberNullColumn7();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(doubleNumberNullColumn7));

        FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName, config().sliceLengthForPLI, spark);
        FPLI PLI = pliConstructor.construct(tableDatasetMap);

    }
}