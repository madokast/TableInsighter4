package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.SerializableConsumer;
import com.sics.rock.tableinsight4.rule.FRule;

import java.util.List;

/**
 * @author zhaorx
 */
public interface FIEvidenceSet {

    List<FRule> applyOn(List<FRule> rules);

    long allCount();

    long[] predicateSupport();

    String[] info(int limit);

    void foreach(SerializableConsumer<FIPredicateSet> psConsumer);
}

