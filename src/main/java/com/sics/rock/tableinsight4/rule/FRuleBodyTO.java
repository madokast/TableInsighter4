package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * compressed rule body (lhs + rhs) for broadcast
 * <p>
 * a rule-body compresses 10^4 ~ 10^6 rules
 * by holding the left hand side bitset / xs and right hand side / y of each rule
 * note that the fields support and xSupport are excluded.
 * <p>
 * when predicateSize = 200 ~ 300, rule number > 10^4
 * the compression ratio can reach to 30% ~ 40%
 *
 * @author zhaorx
 */
public class FRuleBodyTO implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FRuleBodyTO.class);

    /**
     * long array holds left hand side bitset / xs
     */
    private final long[] lhsData;

    /**
     * short array holds right hand side / y
     */
    private final short[] rhsData;

    /**
     * the length of each left hand side bitset in the form of long array
     */
    private final int lhsArrayLength;

    private FRuleBodyTO(List<FRule> rules) {
        int size = rules.size();

        lhsArrayLength = rules.get(0).xs.wordsLength();
        lhsData = new long[size * lhsArrayLength];
        rhsData = new short[size];

        for (int i = 0; i < size; i++) {
            FRule rule = rules.get(i);
            long[] xsArr = rule.xs.getWords();

            System.arraycopy(xsArr, 0, lhsData, lhsArrayLength * i, lhsArrayLength);
            rhsData[i] = (short) rule.y;
        }
    }

    public static FRuleBodyTO create(List<FRule> rules, boolean ruleRunning) {
        if (ruleRunning) {
            FAssertUtils.require(() -> !rules.isEmpty(), "Rule lise is empty");
            FAssertUtils.require(() -> rules.stream().allMatch(FRule::isAllZero), "Rule has been executed");
            FAssertUtils.require(() -> rules.stream().mapToInt(r -> r.xs.wordsLength()).distinct().count() == 1,
                    "The x bitset length of the rules are inconsistent");
        }

        return new FRuleBodyTO(rules);
    }


    public FBitSet ruleLhs(int ruleId) {
        int from = ruleId * lhsArrayLength;
        return FBitSet.of(Arrays.copyOfRange(lhsData, from, from + lhsArrayLength));
    }

    public int ruleRhs(int ruleId) {
        return rhsData[ruleId];
    }

    public int getRuleNumber() {
        return rhsData.length;
    }
}
