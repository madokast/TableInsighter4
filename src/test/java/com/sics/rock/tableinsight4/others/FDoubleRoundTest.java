package com.sics.rock.tableinsight4.others;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * It seems a big decimal can not get exponent form string easily.
 */
public class FDoubleRoundTest extends FBasicTestEnv {


    @Test
    public void test() {
        //22:25:38.398 [main] INFO  TEST - double a is 1.1
        //22:25:38.404 [main] INFO  TEST - bigDecimal a is 1.100000000000000
        //22:25:38.404 [main] INFO  TEST - bigDecimal a toPlainString is 1.100000000000000
        //22:25:38.404 [main] INFO  TEST - bigDecimal a toEngineeringString is 1.100000000000000
        double a = 1.1;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        logger.info("bigDecimal a is " + bigDecimal);
        logger.info("bigDecimal a toPlainString is " + bigDecimal.toPlainString());
        logger.info("bigDecimal a toEngineeringString is " + bigDecimal.toEngineeringString());
    }

    @Test
    public void test2() {
        //22:26:15.292 [main] INFO  TEST - double a is 1.1111111111111112
        //22:26:15.298 [main] INFO  TEST - bigDecimal a is 1.111111111111111
        //22:26:15.298 [main] INFO  TEST - bigDecimal a toPlainString is 1.111111111111111
        //22:26:15.299 [main] INFO  TEST - bigDecimal a toEngineeringString is 1.111111111111111
        double a = 1.11111111111111111111;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        logger.info("bigDecimal a is " + bigDecimal);
        logger.info("bigDecimal a toPlainString is " + bigDecimal.toPlainString());
        logger.info("bigDecimal a toEngineeringString is " + bigDecimal.toEngineeringString());
    }

    @Test
    public void test_round1() {
        double a = 1.11111111111111111111;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        final BigDecimal a0000 = bigDecimal.multiply(BigDecimal.valueOf(1000), MathContext.DECIMAL64);
        logger.info("a0000 a is " + a0000);
        final BigDecimal roundA0000 = a0000.setScale(2, BigDecimal.ROUND_HALF_UP);
        logger.info("roundA0000 a is " + roundA0000);
    }

    @Test
    public void test_round2() {
        double a = 123.314;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        final BigDecimal roundA0000 = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP);
        logger.info("roundA a is " + roundA0000);
    }

    @Test
    public void test_round3() {
        //09:38:44.356 [main] INFO  TEST - double a is 1.23314E102
        //09:38:44.365 [main] INFO  TEST - roundA a is 1233140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.00
        //09:38:44.365 [main] INFO  TEST - roundA a is 1233140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.00
        //09:38:44.365 [main] INFO  TEST - roundA a is 1233140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.00
        double a = 123.314e100;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        final BigDecimal roundA0000 = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP);
        logger.info("roundA a is " + roundA0000.toEngineeringString());
        logger.info("roundA a is " + roundA0000.toPlainString());
        logger.info("roundA a is " + roundA0000.toString());
    }

    @Test
    public void test_round4() {
        //09:39:32.539 [main] INFO  TEST - double a is 1.23314E-98
        //09:39:32.545 [main] INFO  TEST - bigDecimal a is 12.33140000000000E-99
        //09:39:32.545 [main] INFO  TEST - bigDecimal a is 0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001233140000000000
        //09:39:32.545 [main] INFO  TEST - bigDecimal a is 1.233140000000000E-98
        //09:39:32.545 [main] INFO  TEST - roundA a is 0.00
        //09:39:32.545 [main] INFO  TEST - roundA a is 0.00
        //09:39:32.545 [main] INFO  TEST - roundA a is 0.00
        double a = 123.314e-100;
        logger.info("double a is " + a);
        final BigDecimal bigDecimal = new BigDecimal(a, MathContext.DECIMAL64);
        logger.info("bigDecimal a is " + bigDecimal.toEngineeringString());
        logger.info("bigDecimal a is " + bigDecimal.toPlainString());
        logger.info("bigDecimal a is " + bigDecimal.toString());
        final BigDecimal roundA0000 = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP);
        logger.info("roundA a is " + roundA0000.toEngineeringString());
        logger.info("roundA a is " + roundA0000.toPlainString());
        logger.info("roundA a is " + roundA0000.toString());
    }

    @Test
    public void test_round5(){
        double a1 = 123.314e-100;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round6(){
        double a1 = -123.314e-100;
        // TODO
        //09:50:43.536 [main] INFO  TEST - -1.23314E-98 -> -0
        //09:50:43.541 [main] INFO  TEST - -1.23314E-98 -> -0.0
        //09:50:43.542 [main] INFO  TEST - -1.23314E-98 -> -0.00
        //09:50:43.542 [main] INFO  TEST - -1.23314E-98 -> -0.000
        //09:50:43.542 [main] INFO  TEST - -1.23314E-98 -> -0.0000
        //09:50:43.542 [main] INFO  TEST - -1.23314E-98 -> -1E-98
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round7(){
        double a1 = 123.314e+100;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round8(){
        double a1 = 123.314e+100;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round9(){
        double a1 = 1.1;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round10(){
        double a1 = Double.NaN;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round11(){
        double a1 = Double.NEGATIVE_INFINITY;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }

    @Test
    public void test_round12(){
        double a1 = Double.POSITIVE_INFINITY;
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,false));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,false));

        logger.info("{} -> {}", a1, FTiUtils.round(a1, 0,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 1,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 2,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 3,true));
        logger.info("{} -> {}", a1, FTiUtils.round(a1, 4,true));
    }
}
