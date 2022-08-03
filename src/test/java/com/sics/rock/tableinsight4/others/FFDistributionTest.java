package com.sics.rock.tableinsight4.others;

import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.apache.commons.math3.distribution.FDistribution;
import org.junit.Test;

public class FFDistributionTest extends FBasicTestEnv {

    @Test
    public void test() {
        double n = 100000;
        double x = 100000 * 0.05;
        double c = 0.90;

        double v1 = 2 * (n - x + 1);
        double v2 = 2 * x;

        double fl = new FDistribution(v1, v2).inverseCumulativeProbability(c);

        double pl = v2 / (v2 + v1 * fl);

        v1 = 2 * (n - x);
        v2 = 2 * (x + 1);

        double fu = new FDistribution(v2, v1).inverseCumulativeProbability(c);

        double pu = v2 / (v2 + v1 / fu);

        logger.info("test/appear {}/{}, prob = {}~{}", n, x, pl, pu);
    }

}
