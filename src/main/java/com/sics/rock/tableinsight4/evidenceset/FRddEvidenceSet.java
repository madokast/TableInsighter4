package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.FIPredicateSet;
import com.sics.rock.tableinsight4.internal.FPair;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

/**
 * @author zhaorx
 */
public class FRddEvidenceSet implements FIEvidenceSet {

    private static final Logger logger = LoggerFactory.getLogger(FRddEvidenceSet.class);

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

        FRuleBodyTO to = FRuleBodyTO.create(rules, true);
        Broadcast<FRuleBodyTO> toBroadcast = sc.broadcast(to);

        long[] reducedSupportInfo = ES.mapPartitions(psIter -> {
            FRuleBodyTO ruleBodyTOB = toBroadcast.getValue();
            int ruleSize = ruleBodyTOB.getRuleNumber();

            // supportInfos[:ruleSize] = x-support
            // supportInfos[ruleSize+1:] = rule-support
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

    /**
     * find examples which meet the rule and rule.x but y
     * pair._k = predicate-sets meeting rule
     * pair._k = predicate-sets meeting rule.x but y
     */
    @Override
    public FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] examples(final List<FRule> rules, final int limit) {
        FRuleBodyTO to = FRuleBodyTO.create(rules, false);
        Broadcast<FRuleBodyTO> toBroadcast = sc.broadcast(to);

        return ES.mapPartitions(psIter -> {
            FRuleBodyTO ruleBodyTOB = toBroadcast.getValue();
            int ruleSize = ruleBodyTOB.getRuleNumber();

            @SuppressWarnings("unchecked") final FPair<List<FIPredicateSet>, List<FIPredicateSet>>[] res = new FPair[ruleSize];

            while (psIter.hasNext()) {
                FIPredicateSet ps = psIter.next();
                FBitSet psBit = ps.getBitSet();
                IntStream.range(0, ruleSize).parallel().forEach(i -> {
                    FBitSet xs = ruleBodyTOB.ruleLhs(i);
                    int y = ruleBodyTOB.ruleRhs(i);
                    if (xs.isSubSetOf(psBit)) {
                        if (res[i] == null) res[i] = new FPair<>(new ArrayList<>(), new ArrayList<>());
                        if (psBit.get(y)) {
                            final List<FIPredicateSet> ruleExamples = res[i]._k;
                            if (ruleExamples.size() < limit) ruleExamples.add(ps);
                        } else {
                            final List<FIPredicateSet> ruleButYExamples = res[i]._v;
                            if (ruleButYExamples.size() < limit) ruleButYExamples.add(ps);
                        }
                    }

                });
            }
            return Collections.singletonList(res).iterator();
        }).reduce((l1, l2) -> {
            for (int i = 0; i < l1.length; i++) {
                // examples of rule-i
                FPair<List<FIPredicateSet>, List<FIPredicateSet>> p1 = l1[i];
                final FPair<List<FIPredicateSet>, List<FIPredicateSet>> p2 = l2[i];

                // check p1 null and write back
                if (p1 == null) {
                    p1 = new FPair<>(new ArrayList<>(), new ArrayList<>());
                    l1[i] = p1;
                }

                if (p1._k.size() < limit && p2 != null && p2._k != null) {
                    final List<FIPredicateSet> p2Val = p2._k.subList(0, Math.min(p2._k.size(), limit - p1._k.size()));
                    p1._k.addAll(p2Val);
                }
                if (p1._v.size() < limit && p2 != null && p2._v != null) {
                    final List<FIPredicateSet> p2Val = p2._v.subList(0, Math.min(p2._v.size(), limit - p1._v.size()));
                    p1._v.addAll(p2Val);
                }
            }

            return l1;
        });
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
