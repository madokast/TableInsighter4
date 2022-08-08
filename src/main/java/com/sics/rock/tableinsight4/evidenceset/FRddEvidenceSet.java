package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.FSerializableConsumer;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.rule.FRule;
import com.sics.rock.tableinsight4.rule.FRuleBodyTO;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

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
    public void applyOn(List<FRule> rules) {
        FAssertUtils.require(() -> rules.stream().allMatch(FRule::isAllZero), "Verify non-zero rules");

        FRuleBodyTO to = new FRuleBodyTO(rules);
        Broadcast<FRuleBodyTO> toBroadcast = sc.broadcast(to);

        long[] reducedSupportInfo = ES.mapPartitions(psIter -> {
            FRuleBodyTO ruleBodyTOB = toBroadcast.getValue();
            int ruleSize = ruleBodyTOB.getRuleNumber();

            // supportInfos 前 ruleSize 放 xSupp，后 ruleSize 放 supp
            long[] supportInfos = new long[ruleSize * 2];
            Arrays.fill(supportInfos, 0L);

            while (psIter.hasNext()) {
                FIPredicateSet ps = psIter.next();
                FBitSet psBit = ps.getBitSet();
                long count = ps.getSupport();
                IntStream.range(0, ruleSize).parallel().forEach(i -> {
                    FBitSet xs = ruleBodyTOB.ruleLhs(i);
                    int y = ruleBodyTOB.ruleRhs(i);
                    if (xs.isSubSetOf(psBit)) {
                        // xSupp
                        supportInfos[i] += count;
                        if (psBit.get(y)) {
                            // supp
                            supportInfos[i + ruleSize] += count;
                        }
                    }
                });
            }
            return Collections.singletonList(supportInfos).iterator();
        }).reduce(FTiUtils::longArrSum);

        int size = rules.size();
        for (int i = 0; i < size; i++) {
            FRule rule = rules.get(i);
            rule.xSupport = reducedSupportInfo[i];
            rule.support = reducedSupportInfo[i + size];
        }
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
    public void foreach(FSerializableConsumer<FIPredicateSet> psConsumer) {
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
