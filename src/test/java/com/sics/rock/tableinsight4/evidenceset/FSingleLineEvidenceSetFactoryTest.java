package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FPredicateFactory;
import com.sics.rock.tableinsight4.procedure.FConstantHandler;
import com.sics.rock.tableinsight4.procedure.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.procedure.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.procedure.FTableDataLoader;
import com.sics.rock.tableinsight4.procedure.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * TODO waiting for complete test
 */
public class FSingleLineEvidenceSetFactoryTest extends FTableInsightEnv {


    @Test
    public void test() {

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "2"
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

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);
        FPredicateFactory singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler);

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

    }


}