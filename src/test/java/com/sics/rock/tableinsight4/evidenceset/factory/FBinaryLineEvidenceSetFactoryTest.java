package com.sics.rock.tableinsight4.evidenceset.factory;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactoryBuilder;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.apache.spark.util.LongAccumulator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FBinaryLineEvidenceSetFactoryTest extends FTableInsightEnv {

    @Test
    public void test_binary_pred() {

        config().sliceLengthForPLI = 2;
        config().constPredicateCrossLine = false;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,24"
                });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

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

        final FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleTableCrossLinePredicate().use(table, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        final int p_name_i = singleLinePredicateFactory.findIndex("name").get(0);
        final int p_age_i = singleLinePredicateFactory.findIndex("age").get(0);

        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, singleLinePredicateFactory, table.getLength(null));

        ES.foreach(ps -> {
            FBitSet bs = ps.getBitSet();

            if (bs.get(p_age_i)) Assert.assertTrue(bs.get(p_name_i));
        });

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());
        logger.info("predicateSupport = {}", Arrays.toString(ES.predicateSupport()));

        Assert.assertArrayEquals(new long[]{12, 6}, ES.predicateSupport());

        for (String ps : info) {
            logger.info(ps);
        }

    }

    @Test
    public void test_constant_pred() {

        config().sliceLengthForPLI = 3;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,24"
                });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

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

        final FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleTableCrossLinePredicate().use(table, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        //+---------------+--------+---+
        //|         row_id|    name|age|
        //+---------------+--------+---+
        //|132349417164610|zhangsan| 26|
        //|132349417164611|zhangsan| 26|
        //|132349417164612|zhangsan| 26|
        //|132349417164613|zhangsan| 24|
        //+---------------+--------+---+

        // config().sliceLengthForPLI = 3;
        //+---------------+----------+--------+-------+---+------+
        //|row_id         |pid:offset|name    |name_id|age|age_id|
        //+---------------+----------+--------+-------+---+------+
        //|132349417164610|0:0       |zhangsan|2      |26 |1     |
        //|132349417164611|0:1       |zhangsan|2      |26 |1     |
        //|132349417164612|0:2       |zhangsan|2      |26 |1     |
        //|132349417164613|1:0       |zhangsan|2      |24 |0     |
        //+---------------+----------+--------+-------+---+------+

        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = t1.name t0.age = '24' t1.age = '26']↑3
        // [[(1:0x0:0),(1:0x0:1),(1:0x0:2)]]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = t1.name t1.age = '24' t0.age = '26']↑3
        // [[(0:0x1:0),(0:1x1:0),(0:2x1:0)]]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = t1.name t0.age = '26' t1.age = '26' t0.age = t1.age]↑6
        // [[(0:1x0:0),(0:2x0:0),(0:0x0:1),(0:2x0:1),(0:0x0:2),(0:1x0:2)]]

        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, singleLinePredicateFactory, table.getLength(null));

        final LongAccumulator longAccumulator = spark.sparkContext().longAccumulator();

        ES.foreach(ps -> {
            longAccumulator.add(ps.getSupport());
            final String pss = ps.toString(singleLinePredicateFactory);

            Assert.assertTrue(pss.contains("t0.name = 'zhangsan'"));
            Assert.assertTrue(pss.contains("t1.name = 'zhangsan'"));
            Assert.assertTrue(pss.contains("t0.name = t1.name"));

            if (pss.contains("t0.age = '24'") && pss.contains("t1.age = '26'")) Assert.assertEquals(3, ps.getSupport());
            if (pss.contains("t1.age = '24'") && pss.contains("t0.age = '26'")) Assert.assertEquals(3, ps.getSupport());
            if (pss.contains("t0.age = t1.age")) Assert.assertEquals(6, ps.getSupport());
        });

        Assert.assertEquals(table.getLength(null) * (table.getLength(null) - 1), longAccumulator.value().intValue());

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());
        logger.info("predicateSupport = {}", Arrays.toString(ES.predicateSupport()));

        for (String ps : info) {
            logger.info(ps);
        }

    }

    @Test
    public void test_interval_pred() {
        config().sliceLengthForPLI = 1;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,21",
                        "zhangsan,22",
                        "zhangsan1,23",
                        "zhangsan1,24"
                });

        for (final FColumnInfo column : table.getColumns()) {
            if (column.getColumnName().equals("age")) {
                column.getIntervalConstantInfo().addExternalIntervalConstant(">20");
                column.getIntervalConstantInfo().addExternalIntervalConstant("<=22");
            }
        }

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

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

        final FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleTableCrossLinePredicate().use(table, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        //+--------------+---------+---+
        //|        row_id|     name|age|
        //+--------------+---------+---+
        //|49374944011824| zhangsan| 21|
        //|49374944011825| zhangsan| 22|
        //|49374944011826|zhangsan1| 23|
        //|49374944011827|zhangsan1| 24|
        //+--------------+---------+---+
        //+--------------+----------+---------+-------+---+------+
        //|row_id        |pid:offset|name     |name_id|age|age_id|
        //+--------------+----------+---------+-------+---+------+
        //|49374944011824|0:0       |zhangsan |5      |21 |1     |
        //|49374944011825|1:0       |zhangsan |5      |22 |2     |
        //|49374944011826|2:0       |zhangsan1|6      |23 |3     |
        //|49374944011827|3:0       |zhangsan1|6      |24 |4     |
        //+--------------+----------+---------+-------+---+------+


        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, singleLinePredicateFactory, table.getLength(null));

        final LongAccumulator longAccumulator = spark.sparkContext().longAccumulator();

        ES.foreach(ps -> {
            longAccumulator.add(ps.getSupport());
            final String pss = ps.toString(singleLinePredicateFactory);

            Assert.assertTrue(pss.contains("> '20'"));

            if (pss.contains("t0.age = '22'")) Assert.assertTrue(pss.contains("<= '22'"));
            if (pss.contains("t1.age = '22'")) Assert.assertTrue(pss.contains("<= '22'"));
            if (pss.contains("t0.age = '21'")) Assert.assertTrue(pss.contains("<= '22'"));
            if (pss.contains("t1.age = '21'")) Assert.assertTrue(pss.contains("<= '22'"));
        });

        Assert.assertEquals(table.getLength(null) * (table.getLength(null) - 1), longAccumulator.value().intValue());

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());
        logger.info("predicateSupport = {}", Arrays.toString(ES.predicateSupport()));

        for (String ps : info) {
            logger.info(ps);
        }

    }

    @Test
    public void test_binary_constant_pred() {

        config().sliceLengthForPLI = 1;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,26",
                        "zhangsan,26",
                        "zhangsan,24",
                        "lisi,24",
                });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

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

        final FPredicateIndexer crossLinePredicates = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleTableCrossLinePredicate().use(table, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : crossLinePredicates.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        //+--------------+--------+---+
        //|        row_id|    name|age|
        //+--------------+--------+---+
        //|68899865330348|zhangsan| 26|
        //|68899865330349|zhangsan| 26|
        //|68899865330350|zhangsan| 24|
        //|68899865330351|    lisi| 24|
        //+--------------+--------+---+

        // config().sliceLengthForPLI = 3;
        //+--------------+----------+--------+-------+---+------+
        //|row_id        |pid:offset|name    |name_id|age|age_id|
        //+--------------+----------+--------+-------+---+------+
        //|68899865330348|0:0       |zhangsan|0      |26 |1     |
        //|68899865330349|0:1       |zhangsan|0      |26 |1     |
        //|68899865330350|0:2       |zhangsan|0      |24 |0     |
        //|68899865330351|1:0       |lisi    |1      |24 |0     |
        //+--------------+----------+--------+-------+---+------+

        //[t1.name = 'zhangsan' t0.name = 'lisi' t0.age = '24' t1.age = '24' t0.age = '24' ^ t1.age = '24' t0.age = t1.age]↑1[(1:0x0:2)]
        //[t1.name = 'zhangsan' t0.name = 'lisi' t1.age = '26' t0.age = '24']↑2[(1:0x0:0),(1:0x0:1)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = 'zhangsan' ^ t1.name = 'zhangsan' t0.name = t1.name t0.age = '26' t1.age = '24']↑2[(0:0x0:2),(0:1x0:2)]
        //[t0.name = 'zhangsan' t1.name = 'lisi' t0.age = '24' t1.age = '24' t0.age = '24' ^ t1.age = '24' t0.age = t1.age]↑1[(0:2x1:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = 'zhangsan' ^ t1.name = 'zhangsan' t0.name = t1.name t1.age = '26' t0.age = '24']↑2[(0:2x0:0),(0:2x0:1)]
        //[t0.name = 'zhangsan' t1.name = 'lisi' t0.age = '26' t1.age = '24']↑2[(0:0x1:0),(0:1x1:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = 'zhangsan' ^ t1.name = 'zhangsan' t0.name = t1.name t0.age = '26' t1.age = '26' t0.age = '26' ^ t1.age = '26' t0.age = t1.age]↑2[(0:1x0:0),(0:0x0:1)]

        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, crossLinePredicates, table.getLength(null));

        final int t024 = crossLinePredicates.findIndex("t0.age = '24'").get(0);
        final int t124 = crossLinePredicates.findIndex("t1.age = '24'").get(0);
        final int t0124 = crossLinePredicates.findIndex("t01.age = '24'").get(0);

        logger.info("t024 = {}", crossLinePredicates.getPredicate(t024));
        logger.info("t124 = {}", crossLinePredicates.getPredicate(t124));
        logger.info("t0124 = {}", crossLinePredicates.getPredicate(t0124));

        final LongAccumulator longAccumulator = spark.sparkContext().longAccumulator();

        ES.foreach(ps -> {
            longAccumulator.add(ps.getSupport());
            final String pss = ps.toString(crossLinePredicates);

            Assert.assertTrue(pss.contains("zhangsan"));

            final FBitSet bs = ps.getBitSet();

            if (bs.get(t024) && bs.get(t124)) Assert.assertTrue(bs.get(t0124));
            if (bs.get(t0124)) Assert.assertTrue(bs.get(t024));
            if (bs.get(t0124)) Assert.assertTrue(bs.get(t124));
        });

        Assert.assertEquals(table.getLength(null) * (table.getLength(null) - 1), longAccumulator.value().intValue());

        String[] info = ES.info(crossLinePredicates, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());
        logger.info("predicateSupport = {}", Arrays.toString(ES.predicateSupport()));

        for (String ps : info) {
            logger.info(ps);
        }

    }

    @Test
    public void test_binary_interval_pred() {
        config().sliceLengthForPLI = 1;

        final FTableInfo table = FExamples.create("two-row", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,21",
                        "zhangsan,22",
                        "zhangsan1,23",
                        "zhangsan1,24"
                });

        for (final FColumnInfo column : table.getColumns()) {
            if (column.getColumnName().equals("age")) {
                column.getIntervalConstantInfo().addExternalIntervalConstant(">20");
                column.getIntervalConstantInfo().addExternalIntervalConstant("<=22");
            }
        }

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(Collections.singletonList(table));

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

        final FPredicateIndexer singleLinePredicateFactory = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                .buildForSingleTableCrossLinePredicate().use(table, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        //+--------------+---------+---+
        //|        row_id|     name|age|
        //+--------------+---------+---+
        //|49374944011824| zhangsan| 21|
        //|49374944011825| zhangsan| 22|
        //|49374944011826|zhangsan1| 23|
        //|49374944011827|zhangsan1| 24|
        //+--------------+---------+---+
        //+--------------+----------+---------+-------+---+------+
        //|row_id        |pid:offset|name     |name_id|age|age_id|
        //+--------------+----------+---------+-------+---+------+
        //|49374944011824|0:0       |zhangsan |5      |21 |1     |
        //|49374944011825|1:0       |zhangsan |5      |22 |2     |
        //|49374944011826|2:0       |zhangsan1|6      |23 |3     |
        //|49374944011827|3:0       |zhangsan1|6      |24 |4     |
        //+--------------+----------+---------+-------+---+------+


        final FBinaryLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory();

        final FIEvidenceSet ES = evidenceSetFactory.createSingleTableBinaryLineEvidenceSet(
                table, PLI, singleLinePredicateFactory, table.getLength(null));

        final LongAccumulator longAccumulator = spark.sparkContext().longAccumulator();

        final int t0g20 = singleLinePredicateFactory.findIndex("t0.age > '20'").get(0);
        final int t1g20 = singleLinePredicateFactory.findIndex("t1.age > '20'").get(0);
        final int t01g20 = singleLinePredicateFactory.findIndex("t01.age > '20'").get(0);

        final int t0l22 = singleLinePredicateFactory.findIndex("t0.age <= '22'").get(0);
        final int t1l22 = singleLinePredicateFactory.findIndex("t1.age <= '22'").get(0);
        final int t01l22 = singleLinePredicateFactory.findIndex("t01.age <= '22'").get(0);


        ES.foreach(ps -> {
            longAccumulator.add(ps.getSupport());

            final FBitSet bs = ps.getBitSet();

            if (bs.get(t0g20) && bs.get(t1g20)) Assert.assertTrue(bs.get(t01g20));
            if (bs.get(t0l22) && bs.get(t1l22)) Assert.assertTrue(bs.get(t01l22));

            if (bs.get(t01g20)) Assert.assertTrue(bs.get(t0g20));
            if (bs.get(t01g20)) Assert.assertTrue(bs.get(t1g20));

            if (bs.get(t01l22)) Assert.assertTrue(bs.get(t0l22));
            if (bs.get(t01l22)) Assert.assertTrue(bs.get(t1l22));
        });

        Assert.assertEquals(table.getLength(null) * (table.getLength(null) - 1), longAccumulator.value().intValue());

        String[] info = ES.info(singleLinePredicateFactory, 100);

        logger.info("cardinality = {}", ES.cardinality());
        logger.info("allCount = {}", ES.allCount());
        logger.info("predicateSupport = {}", Arrays.toString(ES.predicateSupport()));

        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = 'zhangsan' ^ t1.name = 'zhangsan' t0.name = t1.name t1.age = '22' t0.age = '21' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22' t1.age <= '22' t0.age <= '22' ^ t1.age <= '22']↑1[(0:0x1:0)]
        //[t0.name = 'zhangsan1' t1.name = 'zhangsan1' t0.name = 'zhangsan1' ^ t1.name = 'zhangsan1' t0.name = t1.name t0.age = '24' t1.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20']↑1[(3:0x2:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan1' t0.age = '22' t1.age = '24' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22']↑1[(1:0x3:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan' t0.name = 'zhangsan' ^ t1.name = 'zhangsan' t0.name = t1.name t0.age = '22' t1.age = '21' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22' t1.age <= '22' t0.age <= '22' ^ t1.age <= '22']↑1[(1:0x0:0)]
        //[t1.name = 'zhangsan' t0.name = 'zhangsan1' t1.age = '21' t0.age = '24' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t1.age <= '22']↑1[(3:0x0:0)]
        //[t1.name = 'zhangsan' t0.name = 'zhangsan1' t1.age = '22' t0.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t1.age <= '22']↑1[(2:0x1:0)]
        //[t0.name = 'zhangsan1' t1.name = 'zhangsan1' t0.name = 'zhangsan1' ^ t1.name = 'zhangsan1' t0.name = t1.name t1.age = '24' t0.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20']↑1[(2:0x3:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan1' t0.age = '22' t1.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22']↑1[(1:0x2:0)]
        //[t1.name = 'zhangsan' t0.name = 'zhangsan1' t1.age = '22' t0.age = '24' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t1.age <= '22']↑1[(3:0x1:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan1' t0.age = '21' t1.age = '24' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22']↑1[(0:0x3:0)]
        //[t1.name = 'zhangsan' t0.name = 'zhangsan1' t1.age = '21' t0.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t1.age <= '22']↑1[(2:0x0:0)]
        //[t0.name = 'zhangsan' t1.name = 'zhangsan1' t0.age = '21' t1.age = '23' t0.age > '20' t1.age > '20' t0.age > '20' ^ t1.age > '20' t0.age <= '22']↑1[(0:0x2:0)]

        for (String ps : info) {
            logger.info(ps);
        }

    }
}