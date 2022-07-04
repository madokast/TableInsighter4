package com.sics.rock.tableinsight4.procedure.interval;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

public class FIntervalTest extends FBasicTestEnv {

    private final String I1 = "[3, 4]";
    private final String I2 = "[0.3, .4)";
    private final String I3 = "(10.2, 42.1)";

    @Test
    public void parse_1() {
        logger.info(FInterval.parse(I1).toString());
        logger.info(FInterval.parse(I2).toString());
        logger.info(FInterval.parse(I3).toString());
    }

    @Test
    public void of() {
        logger.info(FInterval.of("5").toString());
        logger.info(FInterval.of("5.1").toString());

        logger.info(FInterval.of(">50").toString());
        logger.info(FInterval.of(">=50").toString());
        logger.info(FInterval.of("<=100.00001").toString());
        logger.info(FInterval.of("<100.00001").toString());

        logger.info(FInterval.of(I1).toString());
        logger.info(FInterval.of(I2).toString());
        logger.info(FInterval.of(I3).toString());
    }

    @Test
    public void inequalityOf() {
        logger.info(FInterval.of("5").get(0).inequalityOf("x"));
        logger.info(FInterval.of("5").get(1).inequalityOf("x"));
        logger.info(FInterval.of("5.1").get(0).inequalityOf("y"));
        logger.info(FInterval.of("5.1").get(1).inequalityOf("y"));

        logger.info(FInterval.of("  >50").get(0).inequalityOf("z"));
        logger.info(FInterval.of("<=  100.00001").get(0).inequalityOf("w"));

        logger.info(FInterval.of(I1).get(0).inequalityOf("name"));
        logger.info(FInterval.of(I2).get(0).inequalityOf("ane v"));
        logger.info(FInterval.of(I3).get(0).inequalityOf("ll ;; ll"));
    }

    @Test
    public void testToString() {
        logger.info(FInterval.of("5").get(0).toString(5, false));
        logger.info(FInterval.of("5").get(1).toString(5, false));
        logger.info(FInterval.of("5.1").get(0).toString(5, false));
        logger.info(FInterval.of("5.1").get(1).toString(5, false));

        logger.info(FInterval.of("  >50").get(0).toString(5, false));
        logger.info(FInterval.of("<=  100.00001").get(0).toString(5, false));

        logger.info(FInterval.of(I1).get(0).toString(5, false));
        logger.info(FInterval.of(I2).get(0).toString(5, false));
        logger.info(FInterval.of(I3).get(0).toString(5, false));
    }

    @Test
    public void including() {
        logger.info("" + FInterval.of("5").get(0).including(5.));
        logger.info("" + FInterval.of("5").get(1).including(5.));
        logger.info("" + FInterval.of("5.1").get(0).including(5.));
        logger.info("" + FInterval.of("5.1").get(1).including(5.));

        logger.info("" + FInterval.of("  >50").get(0).including(5.));
        logger.info("" + FInterval.of("<=  100.00001").get(0).including(5.));

        logger.info("" + FInterval.of(I1).get(0).including(5.));
        logger.info("" + FInterval.of(I2).get(0).including(5.));
        logger.info("" + FInterval.of(I3).get(0).including(5.));
    }
}