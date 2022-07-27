package com.sics.rock.tableinsight4.procedure.constant;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import static org.junit.Assert.*;

public class FConstantTest extends FBasicTestEnv {

    @Test
    public void test_of() {
        logger.info("{}", FConstant.of(null));
        logger.info("{}", FConstant.of(Math.sqrt(-2)));
        logger.info("{}", FConstant.of(1D / 0D));
        logger.info("{}", FConstant.of(-1D / 0D));
        logger.info("{}", FConstant.of(123));
    }

}