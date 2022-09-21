package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FSerializableConsumer;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.rule.FRule;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;

/**
 * An empty evidence-set
 *
 * @author zhaorx
 */
public class FEmptyEvidenceSet implements FIEvidenceSet {

    private static final FEmptyEvidenceSet INS = new FEmptyEvidenceSet();

    public static FEmptyEvidenceSet getInstance() {
        return INS;
    }

    private FEmptyEvidenceSet() {
    }

    @Override
    public long allCount() {
        return 0;
    }

    @Override
    public long cardinality() {
        return 0;
    }

    @Override
    public long[] predicateSupport() {
        return new long[0];
    }

    @Override
    public void applyOn(List<FRule> rules) {
        throw new NotImplementedException("Run rules on empty evidence set");
    }

    @Override
    public FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] examples(final List<FRule> rules, final int limit) {
        throw new NotImplementedException("Find examples on empty evidence set");
    }

    @Override
    public String[] info(int limit) {
        return new String[0];
    }

    @Override
    public String[] info(FPredicateIndexer predicateIndexer, int limit) {
        return new String[0];
    }

    @Override
    public void foreach(FSerializableConsumer<FIPredicateSet> psConsumer) {

    }
}
