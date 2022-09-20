package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.evidenceset.predicateset.FPredicateSet;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FSerializableConsumer;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.rule.FRule;

import java.util.List;

/**
 * @author zhaorx
 */
public interface FIEvidenceSet {

    /**
     * size of the ES
     */
    long allCount();

    /**
     * distinct size of the ES
     */
    long cardinality();

    /**
     * get the support of each predicate
     */
    long[] predicateSupport();

    /**
     * run rules on this evidence set
     */
    void applyOn(List<FRule> rules);

    /**
     * find examples which meet the rule and rule.x but y
     * pair._k = predicate-sets meeting rule
     * pair._k = predicate-sets meeting rule.x but y
     */
    FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] examples(final List<FRule> rules, int limit);

    /**
     * contents of the ES in the form of 01 bitset
     */
    String[] info(int limit);

    /**
     * contents of the ES in the form of predicate
     */
    String[] info(FPredicateIndexer predicateIndexer, int limit);

    /**
     * consume each predicateSet in the ES
     * the method is used for debug only
     */
    void foreach(FSerializableConsumer<FIPredicateSet> psConsumer);
}

