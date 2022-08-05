package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.apache.spark.util.SizeEstimator;
import org.junit.Assert;
import org.junit.Test;

public class FRuleFactoryTest extends FBasicTestEnv {

    @Test
    public void ruleSize() {
        for (int i = 1; i < 300; i += 30) {
            FBitSet x = new FBitSet(i);
            FRule rule = new FRule(x, 0);
            long size = 8 + 4 +
                    // rule ptr + int + long + long + pad
                    4 + 4 + 8 + 8 + 4 +
                    // bitset
                    FBitSet.objectSize(i);

            logger.info("rule size of ps {} is {}", i, size);
            Assert.assertEquals(SizeEstimator.estimate(rule), size);
        }
    }
}