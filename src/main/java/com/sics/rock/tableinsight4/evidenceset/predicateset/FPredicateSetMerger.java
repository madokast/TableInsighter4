package com.sics.rock.tableinsight4.evidenceset.predicateset;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.function.Function2;


/**
 * merge predicate set
 *
 * @author zhaorx
 */
public class FPredicateSetMerger implements Function2<FIPredicateSet, FIPredicateSet, FIPredicateSet> {

    public static final FPredicateSetMerger instance = new FPredicateSetMerger();

    private FPredicateSetMerger() {
    }

    @Override
    public FIPredicateSet call(FIPredicateSet ps1, FIPredicateSet ps2) {

        FAssertUtils.require(() -> ps1.equals(ps2), "Trying merging 2 unequal predicate set " + ps1 + " and " + ps2);

        return new FPredicateSet(ps1.getBitSet(), ps1.getSupport() + ps2.getSupport());

    }

}
