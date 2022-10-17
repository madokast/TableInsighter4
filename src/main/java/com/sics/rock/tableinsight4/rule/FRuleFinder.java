package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSetSearchTree;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.iface.FIConstantPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FRuleFinder implements FIRuleFinder {

    private static final Logger logger = LoggerFactory.getLogger(FRuleFinder.class);

    private final FRuleFactory ruleFactory;
    private final FPredicateIndexer predicateIndexer;
    private final int maxLhsSize;
    private final FIEvidenceSet evidenceSet;

    private long leastSupport;

    /**
     * least confidence of aimed rules
     */
    private final double leastConfidence;

    /**
     * stop finding when the unverified rules number exceeds maxRuleNumber
     */
    private final int maxNeedCreateChildrenRuleNumber;

    /**
     * stop finding when the found rules number exceeds maxRuleNumber
     */
    private final int maxRuleNumber;

    /**
     * The maximum batch number in one rule-find.
     */
    private final int ruleFindMaxBatchNumber;

    /**
     * max running time in ms
     */
    private final long timeout;

    /**
     * max rule number of a batch
     * controlled by rule size and batch size in memory
     */
    private final int batchRuleNumber;

    /**
     * Strong association rules has been found
     */
    private List<FRule> results = new ArrayList<>();

    /**
     * a map holds found rules (rule.y -> rule.xs)
     */
    private Map<Integer, FBitSetSearchTree> resultTree = new HashMap<>();

    /**
     * predicate set whose support is less then the leastSupport
     */
    private FBitSetSearchTree lessSupportPredicateSet = new FBitSetSearchTree();


    FRuleFinder(FRuleFactory ruleFactory, FIEvidenceSet evidenceSet, double cover, double confidence,
                int maxRuleNumber, int maxNeedCreateChildrenRuleNumber, long timeout, long batchHeapSizeMB,
                int ruleFindMaxBatchNumber) {
        this.ruleFactory = ruleFactory;
        this.predicateIndexer = ruleFactory.getPredicateIndexer();
        this.maxLhsSize = ruleFactory.getMaxLhsSize();
        this.evidenceSet = evidenceSet;
        this.leastSupport = (long) Math.floor(evidenceSet.allCount() * cover);
        this.leastConfidence = (float) confidence;
        this.maxNeedCreateChildrenRuleNumber = maxNeedCreateChildrenRuleNumber;
        this.maxRuleNumber = maxRuleNumber;
        this.ruleFindMaxBatchNumber = ruleFindMaxBatchNumber;
        this.timeout = timeout;
        this.batchRuleNumber = (int) (batchHeapSizeMB * 1024 * 1024 / ruleFactory.ruleSize());
        logger.info("Staring rule find");
        logger.info("cover is {}, converting to leastSupport = {}", cover, leastSupport);
        logger.info("max batch size {} MB, converting to batchRuleNumber = {}", batchHeapSizeMB, batchRuleNumber);
    }

    public List<FRule> find() {
        long startTime = System.currentTimeMillis();

        int batchIndex = 1;

        // first generation put into ruleQueue
        Queue<FRule> ruleQueue = new LinkedList<>(ruleFactory.firstGeneration());
        // rules that need create children
        Queue<FRule> needCreateChildren = new LinkedList<>();

        while (!ruleQueue.isEmpty() || !needCreateChildren.isEmpty()) {
            List<FRule> running;

            // take rules to run or create children into queue
            if (!ruleQueue.isEmpty()) {
                running = batchTake(ruleQueue);
            } else {
                ruleQueue.addAll(createChildren(needCreateChildren));
                continue;
            }

            logger.info("Rule find batch {}, verify {} rules", batchIndex, running.size());
            // verify rules
            evidenceSet.applyOn(running);

            // update results、resultTree、lessSupportPs、needCreateChildren
            postProcess(running, needCreateChildren);

            if (stop(startTime, batchIndex)) break;
            batchIndex++;
        }

        logger.info("Rule finding finished. Find {} rules in {} min",
                results.size(), Math.round((System.currentTimeMillis() - startTime) / 60.) / 1000);

        return results;

    }

    private List<FRule> createChildren(Queue<FRule> needCreateChildren) {
        List<FRule> batchChildren = new ArrayList<>(batchRuleNumber + predicateIndexer.size());
        int childNumber = 0;
        while (!needCreateChildren.isEmpty()) {
            FRule father = needCreateChildren.poll();
            List<FRule> children = ruleFactory.createChildrenOf(father, lessSupportPredicateSet, resultTree);
            childNumber += children.size();
            batchChildren.addAll(children);
            if (childNumber >= batchRuleNumber) break;
        }
        return batchChildren;
    }

    /**
     * update results、resultTree、lessSupportPs、needCreateChildren
     * <p>
     * results            = support, confidence and valid (e.g. t0.age = 1 -> t1.age = 1 is invalid)
     * resultTree         = ths same as results
     * needCreateChildren = not into result, xSupport, maxLhsLength
     * lessSupportPs      = predicate set with support less than least
     */
    private void postProcess(List<FRule> verifiedRules, Queue<FRule> needCreateChildren) {
        long startTime = System.currentTimeMillis();

        // counter
        int resultsCt = 0;
        int needCreateChildrenCt = 0;
        int lessSupportPsByXCt = 0;
        int lessSupportPsByXYCt = 0;

        boolean needCreateChildrenOverflowReportFlag = true;

        for (FRule rule : verifiedRules) {
            boolean supportFlag = rule.support >= leastSupport;
            boolean confidenceFlag = rule.confidence() >= leastConfidence;
            boolean xSupportFlag = rule.xSupport >= leastSupport;

            // results & resultTree
            if (supportFlag && confidenceFlag && valid(rule)) {
                resultsCt++;
                results.add(rule);

                FBitSetSearchTree tree = resultTree.getOrDefault(rule.y, new FBitSetSearchTree());
                if (!tree.containSubSet(rule.xs)) tree.add(rule.xs);
                resultTree.put(rule.y, tree);
            }
            // needCreateChildren
            else if (xSupportFlag && rule.lhsLength(predicateIndexer) < maxLhsSize) {
                if (needCreateChildren.size() < maxNeedCreateChildrenRuleNumber) {
                    needCreateChildren.offer(rule);
                    needCreateChildrenCt++;
                } else {
                    if (needCreateChildrenOverflowReportFlag) {
                        logger.warn("The number of rules that need created children exceed {}, stopping put into", maxNeedCreateChildrenRuleNumber);
                        needCreateChildrenOverflowReportFlag = false;
                    }
                }
            }

            // lessSupportPs
            if (!xSupportFlag) {
                if (!lessSupportPredicateSet.containSubSet(rule.xs)) {
                    lessSupportPsByXCt++;
                    lessSupportPredicateSet.add(rule.xs);
                }
            }

            // if not support, put xs^Y into lessSupportPs
            if (!supportFlag) {
                FBitSet xy = rule.xs.copy();
                xy.set(rule.y);
                if (!lessSupportPredicateSet.containSubSet(xy)) {
                    lessSupportPsByXYCt++;
                    lessSupportPredicateSet.add(xy);
                }
            }
        }

        long time = (System.currentTimeMillis() - startTime) / 1000;

        logger.info("post process {} rules in {} s. result (tree) + {} = {}，need-create children + {} = {}, " +
                        "lessSupportPs + {}(X) + {}(XY)",
                verifiedRules.size(), time, resultsCt, results.size(),
                needCreateChildrenCt, needCreateChildren.size(),
                lessSupportPsByXCt, lessSupportPsByXYCt);
    }

    /**
     * invalid type 1:
     * t0.age = 10 -> t1.age = 10
     * 20 < t0.age <= 30 -> 20 < t1.age <= 30
     * <p>
     * invalid type 2:
     * t0.ac = 908.0 ^ t1.ac = 908.0 -> t0.str = t1.str
     * <p>
     * the invalid rules can not put into results but may create children,
     * because some children of them are valid.
     */
    private boolean valid(FRule rule) {
        int y = rule.y;
        FBitSet xs = rule.xs;

        FIPredicate yp = predicateIndexer.getPredicate(y);
        if (yp instanceof FIUnaryPredicate && ((FIUnaryPredicate) yp).tupleIndex() == 1) {
            // if yp is single line predicate and tuple-id is 1, this is multi-line rule finding
            // if there only one predicate in xs, it must be the tuple-id = 0 predicate correspond to yp
            // which is invalid. However, its offspring may be valid. The evaluation of these rules is necessary.
            if (xs.cardinality() == 1) {
                FAssertUtils.require(
                        () -> predicateIndexer.getUnaryPredicateT1ByT0(xs.nextSetBit(0)) == y,
                        () -> "Bad rule " + rule);
                return false;
            }
        }

        // when multi-line rule finding
        if (ruleFactory.isMultiLinePredicateExistence()) {
            // all xs predicates are constant / interval predicates
            if (xs.stream().mapToObj(predicateIndexer::getPredicate).allMatch(x -> x instanceof FIConstantPredicate)) {
                return false;
            }
        }

        return true;
    }

    private boolean stop(long startTime, final int batchIndex) {
        if (results.size() > maxRuleNumber) {
            logger.info("Stop rule finding as the number of result rule {} exceeds {}", results.size(), maxRuleNumber);
            return true;
        }

        if ((System.currentTimeMillis() - startTime) > timeout) {
            logger.info("Stop rule finding as running time exceeds {} min", timeout / 1000. / 60.);
            return true;
        }

        if (batchIndex >= ruleFindMaxBatchNumber) {
            logger.info("Stop rule finding as batch number reaches {}", ruleFindMaxBatchNumber);
            return true;
        }

        return false;
    }

    private List<FRule> batchTake(Queue<FRule> ruleQueue) {
        if (ruleQueue.size() < batchRuleNumber) {
            List<FRule> all = new ArrayList<>(ruleQueue);
            ruleQueue.clear();
            return all;
        }

        List<FRule> take = new ArrayList<>(batchRuleNumber);
        int pollNum = 0;
        while (!ruleQueue.isEmpty()) {
            take.add(ruleQueue.poll());
            pollNum++;
            if (pollNum == batchRuleNumber) break;
        }
        return take;
    }

}
