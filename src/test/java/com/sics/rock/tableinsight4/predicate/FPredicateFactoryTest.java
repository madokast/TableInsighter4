package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnInfoFactory;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.junit.Test;


import java.util.Collections;

import static org.junit.Assert.*;


public class FPredicateFactoryTest extends FTableInsightEnv {


    @Test
    public void createSingleLinePredicateFactory() {

        FTableInfo relation = FExamples.relation();

        for (FColumnInfo column : relation.getColumns()) {
            if (column.getColumnName().equals("cc")) {
                column.addConstant(new FConstant<>(null));
                column.addConstant(new FConstant<>("abc"));
                column.addConstant(new FConstant<>(123));
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

        relation.getColumns().get(0).addConstant(new FConstant<>(null));
        relation.getColumns().get(1).addConstant(new FConstant<>(213));
        relation.getColumns().get(2).addConstant(new FConstant<>("abc"));
        relation.getColumns().get(3).addConstant(new FConstant<>(12.32));

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
        colCombined.addConstant(new FConstant<>("CITY 44312"));
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
                FUtils.listOf("cc", "ac"), Collections.singletonList("ac"),
                true, null, null
        );

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(Collections.singletonList(modelInfo));

        String column = derivedColumnNameHandler.deriveModelColumn(modelId);

        FColumnInfo ci = new FColumnInfo(column, FValueType.LONG);
        ci.addConstant(new FConstant<>(1L));
        relation.getColumns().add(ci);

        FPredicateFactory factory = FPredicateFactory.createSingleLinePredicateFactory(
                relation, derivedColumnNameHandler);

        assertEquals(1, factory.size());

        FIPredicate predicate = factory.getPredicate(0);

        logger.info("innerTabCols = {}", predicate.innerTabCols());

    }
}
