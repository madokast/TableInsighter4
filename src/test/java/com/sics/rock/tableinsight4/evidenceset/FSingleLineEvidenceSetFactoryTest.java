package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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


    @Test
    public void test_constant() {
        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3"
        });

        one.getColumns().get(0).getConstantConfig().addExternalConstants("3");

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

    @Test
    public void test_great() {
        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant(">1");

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

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get();
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get();
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get();
        FIPredicate gt1 = singleLinePredicateFactory.find(">1").get();

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int gt1i = singleLinePredicateFactory.getIndex(gt1);

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(gt1i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertFalse(bitSet.get(gt1i));
            if (bitSet.get(eq2i)) assertTrue(bitSet.get(gt1i));
            if (bitSet.get(eq3i)) assertTrue(bitSet.get(gt1i));
        });
    }

    @Test
    public void test_great_eq() {
        config().sliceLengthForPLI = 1;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant(">=2");

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

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get();
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get();
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get();
        FIPredicate get2 = singleLinePredicateFactory.find(">=2").get();

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int get2i = singleLinePredicateFactory.getIndex(get2);

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(get2i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertFalse(bitSet.get(get2i));
            if (bitSet.get(eq2i)) assertTrue(bitSet.get(get2i));
            if (bitSet.get(eq3i)) assertTrue(bitSet.get(get2i));
        });
    }

}