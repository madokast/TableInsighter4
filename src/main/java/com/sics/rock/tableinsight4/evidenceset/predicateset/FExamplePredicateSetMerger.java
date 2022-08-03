package com.sics.rock.tableinsight4.evidenceset.predicateset;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.function.Function2;

/**
 * merge example predicate set
 *
 * @author zhaorx
 */
public class FExamplePredicateSetMerger implements Function2<FIPredicateSet, FIPredicateSet, FIPredicateSet> {

    private final int positiveNegativeExampleNumber;

    public FExamplePredicateSetMerger(int positiveNegativeExampleNumber) {
        this.positiveNegativeExampleNumber = positiveNegativeExampleNumber;
    }

    @Override
    public FIPredicateSet call(FIPredicateSet ps1, FIPredicateSet ps2) {

        FAssertUtils.require(() -> ps1.equals(ps2), () -> "Trying merging 2 unequal predicate set " + ps1 + " and " + ps2);
        FAssertUtils.require(() -> ps1 instanceof FExamplePredicateSet, () -> "The merged predicate is not ExampleFPredicateSet. " + ps1);
        FAssertUtils.require(() -> ps2 instanceof FExamplePredicateSet, () -> "The merged predicate is not ExampleFPredicateSet. " + ps2);

        final FExamplePredicateSet merged = new FExamplePredicateSet(ps1.getBitSet(), ps1.getSupport() + ps2.getSupport());

        merged.addAll(((FExamplePredicateSet) ps1).getSupportIdsList());

        final int lack = positiveNegativeExampleNumber - merged.supportRowIdsListSize();

        if (lack > 0) ((FExamplePredicateSet) ps2).getSupportIdsList().stream().limit(lack).forEach(merged::add);

        return merged;
    }
}
