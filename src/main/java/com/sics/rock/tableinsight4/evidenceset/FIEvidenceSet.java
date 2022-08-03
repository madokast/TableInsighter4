package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.SerializableConsumer;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.rule.FRule;

import java.util.List;

/**
 * @author zhaorx
 */
public interface FIEvidenceSet {

    /**
     * calculate the support info of rules
     */
    List<FRule> applyOn(List<FRule> rules);

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
    void foreach(SerializableConsumer<FIPredicateSet> psConsumer);
}

