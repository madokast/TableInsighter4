package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.factory.FSingleLineEvidenceSetFactory;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactoryBuilder;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;


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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        assertEquals(one.getLength(null), ES.allCount());

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        assertEquals(one.getLength(null), ES.allCount());

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate gt1 = singleLinePredicateFactory.find(">1").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int gt1i = singleLinePredicateFactory.getIndex(gt1);

        assertEquals(one.getLength(null), ES.allCount());

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        assertEquals(one.getLength(null), ES.allCount());

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate get2 = singleLinePredicateFactory.find(">=2").get(0);

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

    @Test
    public void test_less() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3", "3", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("<2");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate lt2 = singleLinePredicateFactory.find("<2").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int lt2i = singleLinePredicateFactory.getIndex(lt2);

        assertEquals(one.getLength(null), ES.allCount());

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertTrue(bitSet.get(lt2i));
            if (bitSet.get(eq2i)) assertFalse(bitSet.get(lt2i));
            if (bitSet.get(eq3i)) assertFalse(bitSet.get(lt2i));
        });
    }

    @Test
    public void test_less_eq() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3", "3", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("<=2");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate let2 = singleLinePredicateFactory.find("<=2").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int let2i = singleLinePredicateFactory.getIndex(let2);

        long[] supports = ES.predicateSupport();

        logger.info("supports = {}", Arrays.toString(supports));

        assertEquals(one.getLength(null), ES.allCount());

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertTrue(bitSet.get(let2i));
            if (bitSet.get(eq2i)) assertTrue(bitSet.get(let2i));
            if (bitSet.get(eq3i)) assertFalse(bitSet.get(let2i));
        });
    }

    @Test
    public void test_interval_close() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3", "3", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("[2,2]");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate in2 = singleLinePredicateFactory.find("<=2").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int in2i = singleLinePredicateFactory.getIndex(in2);

        long[] supports = ES.predicateSupport();

        logger.info("supports = {}", Arrays.toString(supports));

        assertEquals(one.getLength(null), ES.allCount());

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertFalse(bitSet.get(in2i));
            if (bitSet.get(eq2i)) assertTrue(bitSet.get(in2i));
            if (bitSet.get(eq3i)) assertFalse(bitSet.get(in2i));
        });
    }

    @Test
    public void test_interval_close2() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3", "3", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("[2,3]");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate in23 = singleLinePredicateFactory.find("<=").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int in23i = singleLinePredicateFactory.getIndex(in23);

        long[] supports = ES.predicateSupport();

        logger.info("supports = {}", Arrays.toString(supports));

        assertEquals(one.getLength(null), ES.allCount());

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertFalse(bitSet.get(in23i));
            if (bitSet.get(eq2i)) assertTrue(bitSet.get(in23i));
            if (bitSet.get(eq3i)) assertTrue(bitSet.get(in23i));
        });
    }

    @Test
    public void test_interval_other_close() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                "1", "1", "2", "3", "3", "3"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("[-100,1]");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        for (String ps : ES.info(100)) {
            logger.info(ps);
        }

        FIPredicate eq1 = singleLinePredicateFactory.find("=1").get(0);
        FIPredicate eq2 = singleLinePredicateFactory.find("=2").get(0);
        FIPredicate eq3 = singleLinePredicateFactory.find("=3").get(0);
        FIPredicate l1 = singleLinePredicateFactory.find("<=").get(0);

        int eq1i = singleLinePredicateFactory.getIndex(eq1);
        int eq2i = singleLinePredicateFactory.getIndex(eq2);
        int eq3i = singleLinePredicateFactory.getIndex(eq3);
        int l1i = singleLinePredicateFactory.getIndex(l1);

        long[] supports = ES.predicateSupport();

        logger.info("supports = {}", Arrays.toString(supports));

        assertEquals(one.getLength(null), ES.allCount());

        ES.foreach(ps -> {
            FBitSet bitSet = ps.getBitSet();

            assertFalse(bitSet.get(eq1i) && bitSet.get(eq2i));
            assertFalse(bitSet.get(eq1i) && bitSet.get(eq3i));

            assertFalse(bitSet.get(eq2i) && bitSet.get(eq3i));

            if (bitSet.get(eq1i)) assertTrue(bitSet.get(l1i));
            if (bitSet.get(eq2i)) assertFalse(bitSet.get(l1i));
            if (bitSet.get(eq3i)) assertFalse(bitSet.get(l1i));
        });
    }

    @Test
    public void test_const_special() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                null, "nan", "inf", "-inf", "123"
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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> assertEquals(1, ps.getBitSet().cardinality()));

        String[] info = ES.info(singleLinePredicateFactory, 100);

        assertEquals(one.getLength(null), info.length);

        assertEquals(ES.cardinality(), ES.allCount());

        logger.info("cardinality = {}", ES.cardinality());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_const_card() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                null, "nan", "inf", "-inf", "123", "nan", "null"
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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> assertEquals(1, ps.getBitSet().cardinality()));

        String[] info = ES.info(singleLinePredicateFactory, 100);

        assertEquals(one.getLength(null), info.length + 2);

        assertEquals(ES.cardinality() + 2, ES.allCount());

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_special_compare() {
        config().sliceLengthForPLI = 2;

        FTableInfo one = FExamples.create("one-row", new String[]{"a"}, new FValueType[]{FValueType.DOUBLE}, new String[]{
                null, "nan", "inf", "-inf", "123", "nan", "null"
        });

        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant(">100");
        one.getColumns().get(0).getIntervalConstantInfo().addExternalIntervalConstant("<123");

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
        FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(one, new ArrayList<>()).createPredicates();

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        int eq123 = singleLinePredicateFactory.findIndex("=123").get(0);
        int lt123 = singleLinePredicateFactory.findIndex("<123").get(0);
        int gt100 = singleLinePredicateFactory.findIndex(">100").get(0);
        int eqN_Inf = singleLinePredicateFactory.findIndex("-Inf").get(0);
        int eqP_Inf = singleLinePredicateFactory.findIndex("Inf").stream().filter(i -> i != eqN_Inf).findFirst().get();

        FIEvidenceSet ES = evidenceSetFactory.create(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            if (bs.get(eq123)) assertTrue(bs.get(gt100));
            if (bs.get(eqP_Inf)) assertTrue(bs.get(gt100));
            if (bs.get(eqN_Inf)) assertTrue(bs.get(lt123));
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        assertEquals(one.getLength(null), info.length + 2);

        assertEquals(ES.cardinality() + 2, ES.allCount());

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_single_line_cross_column() {
        config().sliceLengthForPLI = 1;
        config().singleLineCrossColumn = true;
        config().comparableColumnOperators = ">=,>";

        final FTableInfo table = FExamples.create("tab1", new String[]{"age1", "age2"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "22,22",
                        "23,22",
                        "24,23",
                        "25,26"
                });

        final FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(table));

        final FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(Collections.emptyList());

        final FPLI PLI = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark).construct(tableDatasetMap);

        FPredicateIndexer factory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleLinePredicate().use(table, new ArrayList<>()).createPredicates();

        final List<FIPredicate> allPredicates = factory.allPredicates();

        for (final FIPredicate predicate : allPredicates) {
            logger.info("{}", predicate);
        }

        FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);

        final FIEvidenceSet ES = evidenceSetFactory.create(table, PLI, factory, table.getLength(null));

        //+--------------+----+----+
        //|        row_id|age1|age2|
        //+--------------+----+----+
        //|65869765904431|  22|  22|
        //|65869765904432|  23|  22|
        //|65869765904433|  24|  23|
        //|65869765904434|  25|  26|
        //+--------------+----+----+

        //+--------------+----------+----+-------+----+-------+
        //|row_id        |pid:offset|age1|age1_id|age2|age2_id|
        //+--------------+----------+----+-------+----+-------+
        //|65869765904431|0:0       |22  |0      |22  |0      |
        //|65869765904432|1:0       |23  |1      |22  |0      |
        //|65869765904433|2:0       |24  |2      |23  |1      |
        //|65869765904434|3:0       |25  |3      |26  |4      |
        //+--------------+----------+----+-------+----+-------+

        // ES
        // [t0.age1 = t0.age2 t0.age1 >= t0.age2]↑1[(0:0)]
        // [t0.age1 >= t0.age2 t0.age1 > t0.age2]↑2[(1:0),(2:0)]

        int eq = factory.getIndex(allPredicates.stream().filter(p -> p.operator().equals(FOperator.EQ)).findAny().get());
        int get = factory.getIndex(allPredicates.stream().filter(p -> p.operator().equals(FOperator.GET)).findAny().get());
        int gt = factory.getIndex(allPredicates.stream().filter(p -> p.operator().equals(FOperator.GT)).findAny().get());

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            if (bs.get(eq)) assertTrue(bs.get(get));
            if (bs.get(gt)) assertTrue(bs.get(get));
        });

        String[] info = ES.info(factory, 100);

        assertEquals(table.getLength(null), info.length + 1 + 1);

        assertEquals(ES.cardinality() + 1 + 1, ES.allCount());

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }
}