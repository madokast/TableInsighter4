package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.SerializableConsumer;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
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
     * single-line es ==> table-length
     * one table cross-line ==> table-length * (table-length - 1)
     * two table cross-line ==> table1-length * table2-length
     * <p>
     * note: allCount <> rdd.count
     */
    private final long allCount;

    /**
     * support of each predicate
     * <p>
     * lazy init
     */
    private long[] predicateSupport = null;

    /**
     * the distinct size
     * lazy init
     */
    private long cardinality = -1;

    public FRddEvidenceSet(JavaRDD<FIPredicateSet> ES, JavaSparkContext sc, int predicateSize, long allCount) {
        this.ES = ES;
        this.sc = sc;
        this.predicateSize = predicateSize;
        this.allCount = allCount;
    }

    private synchronized void lazyInit(int predSize) {
        if (predicateSupport == null) {
            final long[] reduced = this.ES.mapPartitions(psIter -> {
                // the last element hold the cardinality
                final long[] supports = new long[predSize + 1];
                Arrays.fill(supports, 0L);
                while (psIter.hasNext()) {
                    final FIPredicateSet next = psIter.next();
                    final long support = next.getSupport();
                    next.getBitSet().stream().forEach(x -> supports[x] += support);
                    // cardinality
                    ++supports[predSize];
                }
                return Collections.singletonList(supports).iterator();
            }).reduce(FTiUtils::longArrSum);
            this.predicateSupport = Arrays.copyOf(reduced, predSize);
            this.cardinality = reduced[predSize];
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
    public long cardinality() {
        lazyInit(predicateSize);
        return cardinality;
    }

    @Override
    public long[] predicateSupport() {
        lazyInit(predicateSize);
        return predicateSupport;
    }

    /**
     * test only
     */
    @Override
    public void foreach(SerializableConsumer<FIPredicateSet> psConsumer) {
        ES.foreach(psConsumer::accept);
    }

    /**
     * test only
     */
    @Override
    public String[] info(int limit) {
        return ES.take(limit).stream().map(Objects::toString).toArray(String[]::new);
    }

    /**
     * test only
     */
    @Override
    public String[] info(FPredicateIndexer predicateIndexer, int limit) {
        return ES.take(limit).stream().map(ps -> ps.toString(predicateIndexer)).toArray(String[]::new);
    }
}
