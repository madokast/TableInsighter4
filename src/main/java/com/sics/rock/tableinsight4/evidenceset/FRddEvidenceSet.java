package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.SerializableConsumer;
import com.sics.rock.tableinsight4.rule.FRule;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author zhaorx
 */
public class FRddEvidenceSet implements FIEvidenceSet {

    /**
     * data
     */
    private final JavaRDD<FIPredicateSet> ES;

    private final JavaSparkContext sc;

    /**
     * size of predicate
     */
    private final int predicateSize;

    /**
     * the length of es
     */
    private final long allCount;

    /**
     * support of each predicate
     * <p>
     * lazy init
     */
    private long[] predicateSupport = null;

    public FRddEvidenceSet(JavaRDD<FIPredicateSet> ES, JavaSparkContext sc, int predicateSize, long allCount) {
        this.ES = ES;
        this.sc = sc;
        this.predicateSize = predicateSize;
        this.allCount = allCount;
    }

    private void lazyInit(int predSize) {
        if (predicateSupport == null) {
            this.predicateSupport = this.ES.mapPartitions(psIter -> {
                final long[] supports = new long[predSize];
                Arrays.fill(supports, 0L);
                while (psIter.hasNext()) {
                    final FIPredicateSet next = psIter.next();
                    final long support = next.getSupport();
                    next.getBitSet().stream().forEach(x -> supports[x] += support);
                }
                return Collections.singletonList(supports).iterator();
            }).reduce(FTiUtils::longArrSum);
        }
    }

    @Override
    public List<FRule> applyOn(List<FRule> rules) {
        return null;
    }

    @Override
    public long allCount() {
        return allCount;
    }

    @Override
    public long[] predicateSupport() {
        lazyInit(predicateSize);
        return predicateSupport;
    }

    @Override
    public void foreach(SerializableConsumer<FIPredicateSet> psConsumer) {
        ES.foreach(psConsumer::accept);
    }

    @Override
    public String[] info(int limit) {
        return ES.take(limit).stream().map(Objects::toString).toArray(String[]::new);
    }
}
