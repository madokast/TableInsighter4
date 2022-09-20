package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.apache.spark.util.SizeEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FRuleBodyTOTest extends FBasicTestEnv {

    @Test
    public void test() {

        for (int psLength = 1; psLength < 400; psLength += 30) {
            Random r = new Random();
            int finalPsLength = psLength;
            List<FRule> rules = IntStream.range(0, 1000).mapToObj(i -> {
                FBitSet xs = new FBitSet(finalPsLength);
                int y = r.nextInt(finalPsLength);
                for (int pIndex = 0; pIndex < finalPsLength; pIndex++) {
                    if (r.nextBoolean()) xs.set(pIndex);
                }

                return new FRule(xs, y);
            }).collect(Collectors.toList());

            FRuleBodyTO to = FRuleBodyTO.create(rules, true);

            IntStream.range(0, 1000).forEach(i -> {
                FBitSet xs = to.ruleLhs(i);
                int y = to.ruleRhs(i);
                Assert.assertEquals(rules.get(i).xs, xs);
                Assert.assertEquals(rules.get(i).y, y);

                if (i % 200 == 0) logger.info("to-i {} {} and rule {}", xs.toBriefString(), y, rules.get(i));
            });

            logger.info("pass at " + psLength);
        }
    }

    @Test
    public void test_compression() {
        int predicateSize = 200;
        int ruleNumber = 1;
        int kb = 10;

        while (ruleNumber <= 100_0000) {

            List<FRule> rules = IntStream.range(0, ruleNumber).mapToObj(i -> new FRule(new FBitSet(predicateSize), 0))
                    .collect(Collectors.toList());

            long origin = SizeEstimator.estimate(rules);

            FRuleBodyTO to = FRuleBodyTO.create(rules, true);
            long compress = SizeEstimator.estimate(to);

            double ratio = (double) compress / origin;

            logger.info("rule number {} origin size {} KB compressed size {} KB ratio {}",
                    ruleNumber, origin >> kb, compress >> kb, ratio);

            ruleNumber *= 10;
        }
    }

}