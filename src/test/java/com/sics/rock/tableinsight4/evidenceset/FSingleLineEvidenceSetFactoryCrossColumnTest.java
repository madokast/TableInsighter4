package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryCrossColumnPredicate;
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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FSingleLineEvidenceSetFactoryCrossColumnTest extends FTableInsightEnv {

    @Test
    public void test_special_compare() {
        config().sliceLengthForPLI = 2;

        final FTableInfo one = FExamples.create("two-row", new String[]{"a", "b"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "1,2",
                        "2,3",
                        "4,1",
                        "2,2"
                });

        final String tableName = one.getTableName();
        final String innerTableName = one.getInnerTableName();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final Set<String> a = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "a");
        final Set<String> b = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "b");
        a.addAll(b);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler,
                Collections.singletonList(() -> Collections.singletonList(
                        new FUnaryCrossColumnPredicate(
                                tableName, "a", "b", 0,
                                FOperator.LT, a))
                ));

        final int lt = singleLinePredicateFactory.findIndex("<").get(0);

        final FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);


        final FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            FIPredicate pa = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("a")).findAny().get();
            FIPredicate pb = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("b")).findAny().get();

            int na = (int) (Object) ((FUnaryConsPredicate) pa).constant().getConstant();
            int nb = (int) (Object) ((FUnaryConsPredicate) pb).constant().getConstant();

            if (bs.get(lt)) {
                assertTrue(na < nb);
            } else {
                assertTrue(na >= nb);
            }
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_special_compare_gt() {
        config().sliceLengthForPLI = 2;

        final FTableInfo one = FExamples.create("two-row", new String[]{"a", "b"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "1,2",
                        "2,3",
                        "4,1",
                        "2,2",
                        "5,6",
                        "9,2"
                });

        final String tableName = one.getTableName();
        final String innerTableName = one.getInnerTableName();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final Set<String> a = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "a");
        final Set<String> b = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "b");
        a.addAll(b);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler,
                Collections.singletonList(() -> Collections.singletonList(
                        new FUnaryCrossColumnPredicate(
                                tableName, "a", "b", 0,
                                FOperator.GT, a))
                ));

        final int lt = singleLinePredicateFactory.findIndex(FOperator.GT.symbol).get(0);

        final FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);


        final FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            FIPredicate pa = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("a")).findAny().get();
            FIPredicate pb = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("b")).findAny().get();

            int na = (int) (Object) ((FUnaryConsPredicate) pa).constant().getConstant();
            int nb = (int) (Object) ((FUnaryConsPredicate) pb).constant().getConstant();

            if (bs.get(lt)) {
                assertTrue(na > nb);
            } else {
                assertTrue(na <= nb);
            }
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_special_compare_gte() {
        config().sliceLengthForPLI = 2;

        final FTableInfo one = FExamples.create("two-row", new String[]{"a", "b"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "1,2",
                        "2,3",
                        "4,1",
                        "2,2",
                        "5,6",
                        "9,2",
                        "3,3"
                });

        final String tableName = one.getTableName();
        final String innerTableName = one.getInnerTableName();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final Set<String> a = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "a");
        final Set<String> b = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "b");
        a.addAll(b);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler,
                Collections.singletonList(() -> Collections.singletonList(
                        new FUnaryCrossColumnPredicate(
                                tableName, "a", "b", 0,
                                FOperator.GET, a))
                ));

        final int lt = singleLinePredicateFactory.findIndex(FOperator.GET.symbol).get(0);

        final FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);


        final FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            FIPredicate pa = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("a")).findAny().get();
            FIPredicate pb = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("b")).findAny().get();

            int na = (int) (Object) ((FUnaryConsPredicate) pa).constant().getConstant();
            int nb = (int) (Object) ((FUnaryConsPredicate) pb).constant().getConstant();

            if (bs.get(lt)) {
                assertTrue(na >= nb);
            } else {
                assertTrue(na < nb);
            }
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_special_compare_lte() {
        config().sliceLengthForPLI = 2;

        final FTableInfo one = FExamples.create("two-row", new String[]{"a", "b"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "1,2",
                        "2,3",
                        "4,1",
                        "2,2",
                        "5,6",
                        "9,2",
                        "3,3"
                });

        final String tableName = one.getTableName();
        final String innerTableName = one.getInnerTableName();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final Set<String> a = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "a");
        final Set<String> b = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "b");
        a.addAll(b);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler,
                Collections.singletonList(() -> Collections.singletonList(
                        new FUnaryCrossColumnPredicate(
                                tableName, "a", "b", 0,
                                FOperator.LET, a))
                ));

        final int lt = singleLinePredicateFactory.findIndex(FOperator.LET.symbol).get(0);

        final FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);


        final FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            FIPredicate pa = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("a")).findAny().get();
            FIPredicate pb = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("b")).findAny().get();

            int na = (int) (Object) ((FUnaryConsPredicate) pa).constant().getConstant();
            int nb = (int) (Object) ((FUnaryConsPredicate) pb).constant().getConstant();

            if (bs.get(lt)) {
                assertTrue(na <= nb);
            } else {
                assertTrue(na > nb);
            }
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }

    @Test
    public void test_special_compare_eq() {
        config().sliceLengthForPLI = 2;

        final FTableInfo one = FExamples.create("two-row", new String[]{"a", "b"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "1,2",
                        "2,3",
                        "4,1",
                        "2,2",
                        "5,6",
                        "9,2",
                        "3,3"
                });

        final String tableName = one.getTableName();
        final String innerTableName = one.getInnerTableName();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(one));

        final List<FExternalBinaryModelInfo> externalBinaryModelInfos = Collections.emptyList();
        final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

        final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
        intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

        final FConstantHandler constantHandler = new FConstantHandler();
        constantHandler.generateConstant(tableDatasetMap);

        final FPliConstructor pliConstructor = new FPliConstructor(config().idColumnName,
                config().sliceLengthForPLI, config().positiveNegativeExampleSwitch, spark);
        final FPLI PLI = pliConstructor.construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

        final Set<String> a = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "a");
        final Set<String> b = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, "b");
        a.addAll(b);

        final FPredicateIndexer singleLinePredicateFactory = FPredicateFactory.createSingleLinePredicateFactory(one, derivedColumnNameHandler,
                Collections.singletonList(() -> Collections.singletonList(
                        new FUnaryCrossColumnPredicate(
                                tableName, "a", "b", 0,
                                FOperator.EQ, a))
                ));

        final int lt = singleLinePredicateFactory.findIndex("a=b").get(0);

        final FSingleLineEvidenceSetFactory evidenceSetFactory = new FSingleLineEvidenceSetFactory(
                spark, config().evidenceSetPartitionNumber, config().positiveNegativeExampleSwitch, config().positiveNegativeExampleNumber);


        final FIEvidenceSet ES = evidenceSetFactory.singleLineEvidenceSet(one, PLI, singleLinePredicateFactory, one.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();
            FIPredicate pa = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("a")).findAny().get();
            FIPredicate pb = bs.stream().mapToObj(singleLinePredicateFactory::getPredicate).filter(p -> p.toString().contains("b")).findAny().get();

            int na = (int) (Object) ((FUnaryConsPredicate) pa).constant().getConstant();
            int nb = (int) (Object) ((FUnaryConsPredicate) pb).constant().getConstant();

            if (bs.get(lt)) {
                assertEquals(na, nb);
            } else {
                assertTrue(na != nb);
            }
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());

        for (String ps : info) {
            logger.info(ps);
        }
    }
}
