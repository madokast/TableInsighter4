package com.sics.rock.tableinsight4.procedure.interval;

import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

import java.util.List;
import static org.junit.Assert.*;

public class FIntervalTest extends FBasicTestEnv {

    private final String I1 = "[3, 4]";
    private final String I2 = "[0.3, .4)";
    private final String I3 = "(10.2, 42.1)";

    @Test
    public void parse_1() {
        logger.info(FInterval.parse(I1, FValueType.DOUBLE).toString());
        logger.info(FInterval.parse(I2, FValueType.DOUBLE).toString());
        logger.info(FInterval.parse(I3, FValueType.DOUBLE).toString());
    }

    @Test
    public void of() {
        logger.info(FInterval.of("5", FValueType.DOUBLE).toString());
        logger.info(FInterval.of("5.1", FValueType.DOUBLE).toString());

        logger.info(FInterval.of(">50", FValueType.DOUBLE).toString());
        logger.info(FInterval.of(">=50", FValueType.DOUBLE).toString());
        logger.info(FInterval.of("<=100.00001", FValueType.DOUBLE).toString());
        logger.info(FInterval.of("<100.00001", FValueType.DOUBLE).toString());

        logger.info(FInterval.of(I1, FValueType.DOUBLE).toString());
        logger.info(FInterval.of(I2, FValueType.DOUBLE).toString());
        logger.info(FInterval.of(I3, FValueType.DOUBLE).toString());
    }

    @Test
    public void inequalityOf() {
        logger.info(FInterval.of("5", FValueType.DOUBLE).get(0).inequalityOf("x"));
        logger.info(FInterval.of("5", FValueType.DOUBLE).get(1).inequalityOf("x"));
        logger.info(FInterval.of("5.1", FValueType.DOUBLE).get(0).inequalityOf("y"));
        logger.info(FInterval.of("5.1", FValueType.DOUBLE).get(1).inequalityOf("y"));

        logger.info(FInterval.of("  >50", FValueType.DOUBLE).get(0).inequalityOf("z"));
        logger.info(FInterval.of("<=  100.00001", FValueType.DOUBLE).get(0).inequalityOf("w"));

        logger.info(FInterval.of(I1, FValueType.DOUBLE).get(0).inequalityOf("name"));
        logger.info(FInterval.of(I2, FValueType.DOUBLE).get(0).inequalityOf("ane v"));
        logger.info(FInterval.of(I3, FValueType.DOUBLE).get(0).inequalityOf("ll ;; ll"));
    }

    @Test
    public void testToString() {
        logger.info(FInterval.of("5", FValueType.DOUBLE).get(0).toString(5, false));
        logger.info(FInterval.of("5", FValueType.DOUBLE).get(1).toString(5, false));
        logger.info(FInterval.of("5.1", FValueType.DOUBLE).get(0).toString(5, false));
        logger.info(FInterval.of("5.1", FValueType.DOUBLE).get(1).toString(5, false));

        logger.info(FInterval.of("  >50", FValueType.DOUBLE).get(0).toString(5, false));
        logger.info(FInterval.of("<=  100.00001", FValueType.DOUBLE).get(0).toString(5, false));

        logger.info(FInterval.of(I1, FValueType.DOUBLE).get(0).toString(5, false));
        logger.info(FInterval.of(I2, FValueType.DOUBLE).get(0).toString(5, false));
        logger.info(FInterval.of(I3, FValueType.DOUBLE).get(0).toString(5, false));
    }

    @Test
    public void including() {
        logger.info("" + ((FInterval<Double>) FInterval.of("5", FValueType.DOUBLE).get(0)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of("5", FValueType.DOUBLE).get(1)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of("5.1", FValueType.DOUBLE).get(0)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of("5.1", FValueType.DOUBLE).get(1)).including(5.));

        logger.info("" + ((FInterval<Double>) FInterval.of("  >50", FValueType.DOUBLE).get(0)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of("<=  100.00001", FValueType.DOUBLE).get(0)).including(5.));

        logger.info("" + ((FInterval<Double>) FInterval.of(I1, FValueType.DOUBLE).get(0)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of(I2, FValueType.DOUBLE).get(0)).including(5.));
        logger.info("" + ((FInterval<Double>) FInterval.of(I3, FValueType.DOUBLE).get(0)).including(5.));
    }

    @Test
    public void test_type() {
        List<FInterval<?>> d = FInterval.of("123", FValueType.DOUBLE);
        List<FInterval<?>> s = FInterval.of("123", FValueType.STRING);
        List<FInterval<?>> l = FInterval.of("123", FValueType.LONG);
        List<FInterval<?>> i = FInterval.of("123", FValueType.INTEGER);

        logger.info("type = {}", d);
        logger.info("type = {}", s);
        logger.info("type = {}", l);
        logger.info("type = {}", i);

        assertTrue(d.get(0).constants().get(0).getConstant() instanceof Double);
        assertTrue(s.get(0).constants().get(0).getConstant() instanceof String);
        assertTrue(l.get(0).constants().get(0).getConstant() instanceof Long);
        assertTrue(i.get(0).constants().get(0).getConstant() instanceof Integer);
    }
}