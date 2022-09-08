package com.sics.rock.tableinsight4.preprocessing.constant;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

public class FConstantTest extends FBasicTestEnv {

    @Test
    @SuppressWarnings("all")
    public void test_of() {
        logger.info("{}", FConstant.of(null));
        logger.info("{}", FConstant.of(Math.sqrt(-2)));
        logger.info("{}", FConstant.of(1D / 0D));
        logger.info("{}", FConstant.of(-1D / 0D));
        logger.info("{}", FConstant.of(123));
    }

}