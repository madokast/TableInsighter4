package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;


public class FTypeUtilsTest extends FBasicTestEnv {

    @Test
    public void test() {
        logger.info(Integer.valueOf("1000").toString());
//        logger.info(Integer.valueOf("1000.1").toString());

        logger.info(FTypeUtils.cast("100", Integer.class).toString());
        logger.info(FTypeUtils.cast("100.1", Integer.class).toString());
    }

    @Test
    public void test2() {
        logger.info("nan -> i = {}", (int) (Double.NaN));
        logger.info("-inf -> i = {}", (int) (Double.NEGATIVE_INFINITY));
        logger.info("inf -> i = {}", (int) (Double.POSITIVE_INFINITY));

        logger.info("nan -> i = {}", Double.valueOf(Double.NaN).intValue());
        logger.info("-inf -> i = {}", Double.valueOf(Double.NEGATIVE_INFINITY).intValue());
        logger.info("inf -> i = {}", Double.valueOf(Double.POSITIVE_INFINITY).intValue());
    }
}