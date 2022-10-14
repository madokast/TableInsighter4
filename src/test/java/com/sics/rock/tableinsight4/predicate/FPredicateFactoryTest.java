package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactoryBuilder;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryModelPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryPredicate;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FIExternalBinaryModelCalculator;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FIExternalBinaryModelPredicateNameFormatter;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class FPredicateFactoryTest extends FTableInsightEnv {


    @Test
    public void createSingleLinePredicateFactory() {

        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addConstant(FConstant.of(null));
                column.addConstant(FConstant.of("abc"));
                column.addConstant(FConstant.of(123));
                break;
            }
        }

        FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleLinePredicate().use(
                        relation, new ArrayList<>()).createPredicates();

        assertEquals(3, factory.size());

        assertTrue(factory.getPredicate(0).toString().contains("cc"));
        assertTrue(factory.getPredicate(1).toString().contains("cc"));
        assertTrue(factory.getPredicate(2).toString().contains("cc"));

        assertTrue(factory.getPredicate(0).toString().contains("null"));
        assertTrue(factory.getPredicate(1).toString().contains("abc"));
        assertTrue(factory.getPredicate(2).toString().contains("123"));
    }

    @Test
    public void createSingleLinePredicateFactory2() {

        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            column.addIntervalConstant(new FInterval(1, 2, true, true));
            column.addIntervalConstant(new FInterval(3, 4, true, false));
            column.addIntervalConstant(new FInterval(5, 6, false, false));
            column.addIntervalConstant(new FInterval(Double.NEGATIVE_INFINITY, 2, false, true));
            column.addIntervalConstant(new FInterval(0, Double.POSITIVE_INFINITY, false, false));
            break;
        }

        FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleLinePredicate().use(
                        relation, new ArrayList<>()).createPredicates();

        logger.info("factory {}", factory);
    }

    @Test
    public void createSingleLinePredicateFactory3() {

        FTableInfo relation = FExamples.relation();

        relation.getColumns().get(0).addConstant(FConstant.of(null));
        relation.getColumns().get(1).addConstant(FConstant.of(213));
        relation.getColumns().get(2).addConstant(FConstant.of("abc"));
        relation.getColumns().get(3).addConstant(FConstant.of(12.32));

        FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleLinePredicate().use(relation, new ArrayList<>()).createPredicates();

        assertEquals(4, factory.size());

    }

    @Test
    public void createSingleLinePredicateFactory4() {

        String combineColumnLinker = config().combineColumnLinker;

        FTableInfo relation = FExamples.relation();

        String c0 = relation.getColumns().get(0).getColumnName();
        String c1 = relation.getColumns().get(1).getColumnName();

        FColumnInfo colCombined = new FColumnInfo(c0 + combineColumnLinker + c1, FValueType.STRING);
        colCombined.addConstant(FConstant.of("CITY 44312"));
        relation.getColumns().add(colCombined);

        FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleLinePredicate().use(relation, new ArrayList<>()).createPredicates();

        assertEquals(1, factory.size());

        FIPredicate predicate = factory.getPredicate(0);

        logger.info("innerTabCols = {}", predicate.innerTabCols());

    }

    @Test
    public void createSingleLinePredicateFactory5() {

        FTableInfo relation = FExamples.relation();

        String modelId = "001";

        FExternalBinaryModelInfo modelInfo = new FExternalBinaryModelInfo(
                modelId, relation.getTableName(), relation.getTableName(),
                FTiUtils.listOf("cc", "ac"), Collections.singletonList("ac"),
                true, null, null
        );

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(Collections.singletonList(modelInfo));

        String column = derivedColumnNameHandler.deriveModelColumn(modelId);

        FColumnInfo ci = new FColumnInfo(column, FValueType.LONG);
        ci.addConstant(FConstant.of(1L));
        relation.getColumns().add(ci);

        FPredicateIndexer factory = new FPredicateFactoryBuilder(derivedColumnNameHandler, null, null).buildForSingleLinePredicate().use(
                relation, new ArrayList<>()).createPredicates();

        assertEquals(1, factory.size());

        FIPredicate predicate = factory.getPredicate(0);

        logger.info("innerTabCols = {}", predicate.innerTabCols());

    }

    @Test
    public void createSingleLinePredicateFactory_single_line_cross_column() {

        config().sliceLengthForPLI = 1;
        config().singleLineCrossColumn = true;
        config().comparableColumnOperators = "<=,<";

        final FTableInfo table = FExamples.create("tab1", new String[]{"age1", "age2"},
                new FValueType[]{FValueType.INTEGER, FValueType.INTEGER}, new String[]{
                        "22,21",
                        "23,22",
                        "24,23",
                        "25,24"
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

        final Optional<FIPredicate> lt = allPredicates.stream().filter(p -> p.operator().equals(FOperator.LT)).findAny();
        final Optional<FIPredicate> lte = allPredicates.stream().filter(p -> p.operator().equals(FOperator.LET)).findAny();

        assertTrue(lt.isPresent());
        assertTrue(lte.isPresent());

        assertTrue(lt.get().toString().contains("t0.age1"));
        assertTrue(lt.get().toString().contains("t0.age2"));

        assertTrue(lte.get().toString().contains("t0.age1"));
        assertTrue(lte.get().toString().contains("t0.age2"));

    }

    @Test
    public void createSingleTableCrossLinePredicateFactory_normal_only() {
        config().constPredicateCrossLine = false;

        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addConstant(FConstant.of(null));
                column.addConstant(FConstant.of("abc"));
                column.addConstant(FConstant.of(123));
                break;
            }
        }

        final FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleTableCrossLinePredicate().use(relation, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : factory.allPredicates()) {
            assertTrue(predicate instanceof FBinaryPredicate);
        }
    }

    @Test
    public void createSingleTableCrossLinePredicateFactory_constant() {
        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addConstant(FConstant.of(null));
                column.addConstant(FConstant.of("abc"));
                column.addConstant(FConstant.of(123));
                break;
            }
        }

        final FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleTableCrossLinePredicate().use(relation, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryConsPredicate) {
                assertTrue(predicate.toString().contains("t1"));
            }
            if (predicate instanceof FBinaryIntervalConsPredicate) {
                assertTrue(predicate.toString().contains("t1"));
            }
        }
    }

    @Test
    public void createSingleTableCrossLinePredicateFactory_interval() {
        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addIntervalConstant(new FInterval(10, 20, true, false));
                break;
            }
        }

        final FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(Collections.emptyList()), null, null)
                .buildForSingleTableCrossLinePredicate().use(relation, Collections.emptyList()).createPredicates();

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryConsPredicate) {
                assertTrue(predicate.toString().contains("t1"));
            }
            if (predicate instanceof FBinaryIntervalConsPredicate) {
                assertTrue(predicate.toString().contains("t1"));
            }
        }
    }

    @Test
    public void createSingleTableCrossLinePredicateFactory_binary_model() {
        FTableInfo relation = FExamples.relation();

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap relationDatasetMap = dataLoader.prepareData(FTiUtils.listOf(relation));

        String modelId = "001";
        FIExternalBinaryModelCalculator calculator = () -> {
            List<FPair<Long, Long>> pairs = new ArrayList<>();
            Dataset<Row> dataset = relationDatasetMap.getDatasetByTableName(relation.getTableName());
            List<Row> ccRows = dataset.select(config().idColumnName, "cc").collectAsList();
            for (Row left : ccRows) {
                long leftRowId = left.getLong(0);
                String letCC = left.getString(1);
                for (Row right : ccRows) {
                    long rightRowId = right.getLong(0);
                    String rightCC = right.getString(1);
                    if (Objects.equals(letCC, rightCC)) {
                        pairs.add(new FPair<>(leftRowId, rightRowId));
                    }
                }
            }
            return pairs;
        };

        FIExternalBinaryModelPredicateNameFormatter predicateNameFormatter = (leftTupleId, rightTupleId) ->
                "similar('equal', t0.cc, t1.cc)";

        FExternalBinaryModelInfo modelInfo = new FExternalBinaryModelInfo(modelId, relation.getTableName(), relation.getTableName(),
                FTiUtils.listOf("cc"), FTiUtils.listOf("cc"), true,
                calculator, predicateNameFormatter);
        FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
        modelHandler.appendDerivedColumn(relationDatasetMap, FTiUtils.listOf(modelInfo));

        relationDatasetMap.getDatasetByTableName(relation.getTableName()).show();

        final FPredicateIndexer factory = new FPredicateFactoryBuilder(new FDerivedColumnNameHandler(FTiUtils.listOf(modelInfo)), null, null)
                .buildForSingleTableCrossLinePredicate().use(relation, Collections.emptyList()).createPredicates();

        assertTrue(factory.allPredicates().stream().anyMatch(p -> p instanceof FBinaryModelPredicate));

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryModelPredicate) {
                assertTrue(predicate.innerTabCols().contains(relation.getInnerTableName() + config().tableColumnLinker + "cc"));
            }
        }
    }

    @Test
    public void test_binary_number_cross_pred() {
        config().sliceLengthForPLI = 1;
        config().comparableColumnOperators = "<,>=";

        final FTableInfo table1 = FExamples.create("tab1", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,21",
                        "zhangsan,22",
                        "zhangsan1,23",
                        "zhangsan1,24"
                });

        final FTableInfo table2 = FExamples.create("tab2", new String[]{"name", "age"},
                new FValueType[]{FValueType.STRING, FValueType.INTEGER}, new String[]{
                        "zhangsan,21",
                        "zhangsan,22",
                        "zhangsan1,23",
                        "zhangsan1,24"
                });

        final FTableDataLoader dataLoader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(FTiUtils.listOf(table1, table2));

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
                .buildForBinaryTableCrossLinePredicate()
                .use(table1, table2, Collections.emptyList()).createPredicates();

        //18:15:05.900 [main] INFO  TEST - predicate = t0.name = t1.name
        //18:15:05.900 [main] INFO  TEST - predicate = t0.age = t1.age
        //18:15:05.900 [main] INFO  TEST - predicate = t0.age < t1.age
        //18:15:05.900 [main] INFO  TEST - predicate = t0.age >= t1.age
        for (FIPredicate predicate : singleLinePredicateFactory.allPredicates()) {
            logger.info("predicate = " + predicate);
        }

        final Optional<FIPredicate> ageLT = singleLinePredicateFactory.allPredicates().stream().filter(p -> p.operator().equals(FOperator.LT)).findAny();
        final Optional<FIPredicate> ageGET = singleLinePredicateFactory.allPredicates().stream().filter(p -> p.operator().equals(FOperator.GET)).findAny();

        assertTrue(ageLT.isPresent());
        assertTrue(ageGET.isPresent());

        assertTrue(ageLT.get().toString().contains("t0.age"));
        assertTrue(ageLT.get().toString().contains("t1.age"));
        assertTrue(ageGET.get().toString().contains("t0.age"));
        assertTrue(ageGET.get().toString().contains("t1.age"));

    }
}
