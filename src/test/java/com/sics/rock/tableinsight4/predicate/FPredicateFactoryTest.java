package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.procedure.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.procedure.FTableDataLoader;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.procedure.external.binary.FIExternalBinaryModelCalculator;
import com.sics.rock.tableinsight4.procedure.external.binary.FIExternalBinaryModelPredicateNameFormatter;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;
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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;


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

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, new FDerivedColumnNameHandler(Collections.emptyList()));

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

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, new FDerivedColumnNameHandler(Collections.emptyList()));

    }

    @Test
    public void createSingleLinePredicateFactory3() {

        FTableInfo relation = FExamples.relation();

        relation.getColumns().get(0).addConstant(FConstant.of(null));
        relation.getColumns().get(1).addConstant(FConstant.of(213));
        relation.getColumns().get(2).addConstant(FConstant.of("abc"));
        relation.getColumns().get(3).addConstant(FConstant.of(12.32));

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, new FDerivedColumnNameHandler(Collections.emptyList()));

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

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, new FDerivedColumnNameHandler(Collections.emptyList()));

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

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, derivedColumnNameHandler);

        assertEquals(1, factory.size());

        FIPredicate predicate = factory.getPredicate(0);

        logger.info("innerTabCols = {}", predicate.innerTabCols());

    }

    @Test
    public void createSingleTableCrossLinePredicateFactory_normal_only() {
        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addConstant(FConstant.of(null));
                column.addConstant(FConstant.of("abc"));
                column.addConstant(FConstant.of(123));
                break;
            }
        }

        FPredicateFactory factory = FPredicateFactory.createSingleTableCrossLinePredicateFactory(
                relation, false, new FDerivedColumnNameHandler(Collections.emptyList()));

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

        FPredicateFactory factory = FPredicateFactory.createSingleTableCrossLinePredicateFactory(
                relation, true, new FDerivedColumnNameHandler(Collections.emptyList()));

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryConsPredicate) {
                assertTrue(((FBinaryConsPredicate) predicate).toString().contains("t1"));
            }
            if (predicate instanceof FBinaryIntervalConsPredicate) {
                assertTrue(((FBinaryIntervalConsPredicate) predicate).toString().contains("t1"));
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

        FPredicateFactory factory = FPredicateFactory.createSingleTableCrossLinePredicateFactory(
                relation, true, new FDerivedColumnNameHandler(Collections.emptyList()));

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryConsPredicate) {
                assertTrue(((FBinaryConsPredicate) predicate).toString().contains("t1"));
            }
            if (predicate instanceof FBinaryIntervalConsPredicate) {
                assertTrue(((FBinaryIntervalConsPredicate) predicate).toString().contains("t1"));
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

        FPredicateFactory factory = FPredicateFactory.createSingleTableCrossLinePredicateFactory(
                relation, true, new FDerivedColumnNameHandler(FTiUtils.listOf(modelInfo)));

        assertTrue(factory.allPredicates().stream().anyMatch(p -> p instanceof FBinaryModelPredicate));

        for (FIPredicate predicate : factory.allPredicates()) {
            if (predicate instanceof FBinaryModelPredicate) {
                assertTrue(predicate.innerTabCols().contains(relation.getInnerTableName() + config().tableColumnLinker + "cc"));
            }
        }
    }
}
