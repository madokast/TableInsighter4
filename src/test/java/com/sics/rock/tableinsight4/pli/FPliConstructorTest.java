package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FPliConstructorTest extends FTableInsightEnv {

    @Test
    public void construct() {

        FTableInfo relation = FExamples.relation();

        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        FPliConstructor constructor = new FPliConstructor(
                config().idColumnName,
                2, true,
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
                1000, true,
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

        FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        FPLI PLI = pliConstructor.construct(tableDatasetMap);

    }

    @Test
    public void construct4() {
        config().sliceLengthForPLI = 5;

        FTableInfo doubleNumberNullColumn7 = FExamples.doubleNumberNullColumn7();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(doubleNumberNullColumn7));

        FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        FPLI PLI = pliConstructor.construct(tableDatasetMap);

    }


    @Test
    public void construct_special() {
        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                null, "NaN", "+Inf", "-INF", "123"
        });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        FPLI PLI = pliConstructor.construct(tableDatasetMap);
    }
}