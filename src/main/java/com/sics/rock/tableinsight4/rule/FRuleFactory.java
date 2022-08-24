package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSetSearchTree;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FRuleFactory {

    private static final Logger logger = LoggerFactory.getLogger(FRuleFactory.class);

    private final FPredicateIndexer predicateIndexer;

    /**
     * total predicate number
     * predicateIndexer.size
     */
    private final int predicateSize;

    /**
     * max number of left hand side (lhs/x) predicates
     */
    private final int maxLhsSize;

    /**
     * is there multi-line (binary) predicate in predicateIndexer
     */
    private final boolean multiLinePredicateExistence;

    /**
     * predicates that are able to be use at right hand side (rhs/y)
     * <p>
     * these predicates are record in an int array for iteration
     */
    private final int[] RHSs;

    /**
     * the compatible predicates of each one
     * <p>
     * if 2 predicates have no common column, they are compatible
     * a valid rule is consist of predicates that are compatible with each others
     */
    private final Map<Integer, FBitSet> compatiblePredicatesMap;

    /**
     * predicates that are able to add to rule (left hand side exactly) without consideration about compatibility
     * all predicates are addable when single line rule finding.
     * binary predicates are addable when multi-line (2 lines) rule finding.
     * <p>
     * when finding, a predicate can add to a rule iff. addable & compatible & under size limit
     * <p>
     * these predicates are record in a bitset for fast check
     */
    private final FBitSet addablePredicates;

    /**
     * Create rules for rule finding.
     * This is the first batch of rule finding, so call it first generation.
     * <p>
     * The lhs-size of 1st-generation rules are 1 in most case,
     * such as t0.age = 10 -> t1.age = 10, t0.age = t1.age -> t0.name = t1.name
     * There are also some special 2 lhs-size rules in multi-line (2-line) rule finding,
     * like t0.ahe=1 ^ t1.age=1 -> Y. see class {FBinaryConsPredicate} and {FBinaryIntervalConsPredicate}
     */
    public List<FRule> firstGeneration() {
        List<FRule> rules = new ArrayList<>();
        IntStream.of(RHSs).forEach(y -> {
            FIPredicate yp = predicateIndexer.getPredicate(y);
            // if rhs/y is single line predicate and its tuple-id is 1
            // then lhs must be the relative tuple-id = 0 predicate
            // like t0.age = 10 -> t1.age = 10
            if (yp instanceof FIUnaryPredicate && ((FIUnaryPredicate) yp).tupleIndex() == 1) {
                int x = predicateIndexer.getUnaryPredicateT0ByT1(y);
                FBitSet xBitSet = new FBitSet(predicateSize);
                xBitSet.set(x);
                FRule rule = new FRule(xBitSet, y);
                rules.add(rule);
            } else {
                // case 1: rhs/y is single line predicate and its tuple-id is 0
                //         which means current rule finding is single line rule finding.
                // case 2: rhs/y is a binary predicate
                // In any case, the lhs/x predicates are the compatible and addable predicates of rhs/y
                FBitSet xs = compatiblePredicatesMap.get(y);
                xs.stream().forEach(x -> {
                    // addable only
                    if (addablePredicates.get(x)) {
                        // check length of lhs/predicate if maxLhsSize is 1
                        if (maxLhsSize == 1 && predicateIndexer.getPredicate(x).length() > 1) return;
                        FBitSet xBitSet = new FBitSet(predicateSize);
                        xBitSet.set(x);
                        FRule rule = new FRule(xBitSet, y);
                        rules.add(rule);
                    }
                });
            }
        });
        return rules;
    }

    /**
     * Create all children rules of the father rule.
     * <p>
     * param invalidLhs is the set of less-support lhs in practise
     * param invalidLhsOfRhs is the map from rhs to relative found rules in practise
     *
     * @param invalidLhs      the child rule is invalid if invalidLhs.containSubSet(child.xs)
     * @param invalidLhsOfRhs the child rule is invalid if invalidLhsOfRhs[child.y].containSubSet(child.xs)
     */
    public List<FRule> createChildrenOf(final FRule father, FBitSetSearchTree invalidLhs, Map<Integer, FBitSetSearchTree> invalidLhsOfRhs) {
        final FBitSet xs = father.xs;
        final int y = father.y;

        // check length of lhs
        final int xLen = father.lhsLength(predicateIndexer);
        if (xLen >= maxLhsSize) return Collections.emptyList();

        // flag of to add binary combined predicate or not.
        // see class {FBinaryConsPredicate} and {FBinaryIntervalConsPredicate}
        final int freeLhsSpace = maxLhsSize - xLen;

        // the highest bit in xs.
        final int highest = xs.highest();
        FIPredicate highestPredicate = predicateIndexer.getPredicate(highest);
        FAssertUtils.require(() -> highest >= 0, () -> "xs is empty in rule = " + father);

        // the indexes of new add lhs predicates should be greater then beginIndex for the prevention of repetitive creation.
        // in normal case, the beginIndex is highest.
        // However, in multi-line rule finding
        // if the highest is single-line & tuple-id = 0 predicate, the beginIndex = xs.previousSetBit(beginIndex - 1)
        // The single-line & tuple-id = 0 should be always ignore in the beginIndex determination.
        // The check need no iteration because there are only one single-line & tuple-id = 0 predicate when multi-line rule finding
        final int beginIndex;
        if (multiLinePredicateExistence && highestPredicate instanceof FIUnaryPredicate && ((FIUnaryPredicate) highestPredicate).tupleIndex() == 0) {
            // check something for debug
            FAssertUtils.require(() -> checkUnaryTuple1Rule(father), () -> "Bad rule " + father);
            beginIndex = xs.previousSetBit(highest - 1);
        } else {
            beginIndex = xs.highest();
        }

        List<FRule> children = new ArrayList<>();

        // create children from beginIndex
        for (int addX = beginIndex + 1; addX < predicateSize; addX++) {
            if (addX == highest) continue;
            if (addX == y) continue;
            // space check
            if (predicateIndexer.getPredicate(addX).length() > freeLhsSpace) continue;
            // try create
            Optional<FRule> childOpt = tryCreateChild(father, addX);
            if (!childOpt.isPresent()) continue;
            FRule child = childOpt.get();
            // check child is smallest rule
            if (invalidLhsOfRhs.containsKey(y) && invalidLhsOfRhs.get(y).containSubSet(child.xs)) continue;
            // check the support of child.xs
            if (invalidLhs.containSubSet(child.xs)) continue;
            children.add(child);
        }

        return children;
    }

    /**
     * create child rule from father rule by add one x predicate into lefe hand side.
     * check the valid of the child.
     */
    private Optional<FRule> tryCreateChild(final FRule father, int addX) {
        final FBitSet xs = father.xs;
        final int y = father.y;

        FAssertUtils.require(() -> y != addX, () -> "Bad parameters in tryCreateChild. " + "father = " + father + ", addX = " + addX);
        FAssertUtils.require(() -> !xs.get(addX), () -> "Bad parameters in tryCreateChild. " + "father = " + father + ", addX = " + addX);


        // addX is addable ?
        if (!addablePredicates.get(addX)) return Optional.empty();

        // the compatibility of y and addX
        if (!compatiblePredicatesMap.get(y).get(addX)) return Optional.empty();

        // the compatibility of each xs and addX
        for (int x = xs.nextSetBit(0); x >= 0; x = xs.nextSetBit(x + 1)) {
            if (!compatiblePredicatesMap.get(x).get(addX)) return Optional.empty();
        }

        // create child
        final FBitSet childXs = xs.copy();
        childXs.set(addX);

        final FRule child = new FRule(childXs, y);

        return Optional.of(child);
    }

    /**
     * size of a rule created by this factory
     */
    public long ruleSize() {
//        FBitSet xBitSet = new FBitSet(predicateSize);
//        FRule rule = new FRule(xBitSet, 0);
//        return SizeEstimator.estimate(rule);

        // In 64 bit and pointer compressed
        return
                // rule markWord + clsPtr
                8 + 4 +
                        // rule ptr + ptr + long + long + pad
                        4 + 4 + 8 + 8 + 4 +
                        // bitset
                        FBitSet.objectSize(predicateSize);
    }

    /*======================================== constructor ========================================*/

    public FRuleFactory(FPredicateIndexer predicateIndexer, List<FTableInfo> tableInfos,
                        int maxLeftHandSidePredicateSize, String tableColumnLinker) {
        this.predicateIndexer = predicateIndexer;
        this.predicateSize = predicateIndexer.size();
        this.multiLinePredicateExistence = predicateIndexer.allPredicates().stream().anyMatch(p -> p instanceof FIBinaryPredicate);
        this.maxLhsSize = maxLeftHandSidePredicateSize < 1 ? predicateSize : maxLeftHandSidePredicateSize;

        this.RHSs = createRHSs(predicateIndexer, tableInfos, tableColumnLinker, this.multiLinePredicateExistence);
        this.compatiblePredicatesMap = createCompatiblePredicatesMap(predicateIndexer);
        this.addablePredicates = createAddablePredicates(predicateIndexer, this.multiLinePredicateExistence);
    }

    private static FBitSet createAddablePredicates(FPredicateIndexer predicateIndexer, boolean multiLinePredicateExistence) {
        final int size = predicateIndexer.size();
        final FBitSet addable = new FBitSet(size);
        if (multiLinePredicateExistence) {
            predicateIndexer.allPredicates().stream().filter(
                    // normal binary predicate and binary const/interval predicate
                    predicate -> predicate instanceof FIBinaryPredicate
            ).map(predicateIndexer::getIndex).forEach(addable::set);
        } else {
            addable.fill();
        }

        return addable;
    }

    private Map<Integer, FBitSet> createCompatiblePredicatesMap(FPredicateIndexer predicateIndexer) {
        final int size = predicateIndexer.size();
        final List<FIPredicate> allPredicates = predicateIndexer.allPredicates();

        return allPredicates.stream().map(predicate -> {
            final Set<String> identifiers = predicate.innerTabCols();
            final FBitSet compatiblePredicates = new FBitSet(size);
            next_candidate:
            for (FIPredicate candidate : allPredicates) {
                final Set<String> candidateIds = candidate.innerTabCols();
                for (String candidateId : candidateIds) {
                    if (identifiers.contains(candidateId)) continue next_candidate;
                }
                compatiblePredicates.set(predicateIndexer.getIndex(candidate));
            }
            return new FPair<>(predicateIndexer.getIndex(predicate), compatiblePredicates);
        }).collect(Collectors.toMap(FPair::k, FPair::v));
    }

    private static int[] createRHSs(FPredicateIndexer predicateIndexer, List<FTableInfo> tableInfos,
                                    String tableColumnLinker, boolean multiLinePredicateExistence) {
        Set<String> nonTargets = tableInfos.stream().flatMap(tableInfo -> {
            final String innerTableName = tableInfo.getInnerTableName();
            return tableInfo.nonTargetColumnsView().stream().map(FColumnInfo::getColumnName).map(c -> innerTableName + tableColumnLinker + c);
        }).collect(Collectors.toSet());

        return predicateIndexer.allPredicates().stream().filter(predicate -> {
            // in multi line rule finding, single line const / interval predicate with tuple-id 0 cannot be rhs
            if (multiLinePredicateExistence && predicate instanceof FIUnaryPredicate) {
                if (((FIUnaryPredicate) predicate).tupleIndex() == 0) {
                    logger.debug("Predicate {} cannot be used at right hand side", predicate);
                    return false;
                }
            }

            // binary const / interval predicate cannot be rhs
            if (predicate instanceof FBinaryConsPredicate) {
                logger.debug("Predicate {} cannot be used at right hand side", predicate);
                return false;
            }

            if (predicate instanceof FBinaryIntervalConsPredicate) {
                logger.debug("Predicate {} cannot be used at right hand side", predicate);
                return false;
            }

            // predicate cannot be rhs, if its relative column is non target
            for (String identifier : predicate.innerTabCols()) {
                if (nonTargets.contains(identifier)) {
                    logger.debug("Predicate {} cannot be used at right hand side", predicate);
                    return false;
                }
            }

            return true;
        }).mapToInt(predicateIndexer::getIndex).toArray();


    }

    public FPredicateIndexer getPredicateIndexer() {
        return predicateIndexer;
    }

    public int getPredicateSize() {
        return predicateSize;
    }

    public int getMaxLhsSize() {
        return maxLhsSize;
    }

    public boolean isMultiLinePredicateExistence() {
        return multiLinePredicateExistence;
    }

    public int[] getRHSs() {
        return RHSs;
    }

    public Map<Integer, FBitSet> getCompatiblePredicatesMap() {
        return compatiblePredicatesMap;
    }

    public FBitSet getAddablePredicates() {
        return addablePredicates;
    }

    /**
     * check the rule is like
     * ... ^ t0.col = 123 → t1.col = 123
     * or
     * ... ^ 0 < t0.col <= 12 → 0 < t0.col <= 12
     * <p>
     * for debug only
     */
    private boolean checkUnaryTuple1Rule(FRule rule) {
        final int hx = rule.xs.highest();
        final int y = rule.y;
        final FIPredicate hxp = predicateIndexer.getPredicate(hx);
        final FIPredicate yp = predicateIndexer.getPredicate(y);
        if (!(hxp instanceof FIUnaryPredicate)) return false;
        if (!(yp instanceof FIUnaryPredicate)) return false;
        if (((FIUnaryPredicate) yp).tupleIndex() != 1) return false;
        if (((FIUnaryPredicate) hxp).tupleIndex() != 0) return false;
        if (!hxp.innerTabCols().equals(yp.innerTabCols())) return false;

        if (hxp instanceof FUnaryConsPredicate) {
            if (!(yp instanceof FUnaryConsPredicate)) return false;
            if (!((FUnaryConsPredicate) hxp).allConstants().equals(((FUnaryConsPredicate) yp).allConstants())) {
                return false;
            }
        }
        if (hxp instanceof FUnaryIntervalConsPredicate) {
            if (!(yp instanceof FUnaryIntervalConsPredicate)) return false;
            if (!((FUnaryIntervalConsPredicate) hxp).interval().equals(((FUnaryIntervalConsPredicate) yp).interval())) {
                return false;
            }
        }
        if (hxp instanceof FUnaryCrossColumnPredicate) return false;
        if (yp instanceof FUnaryCrossColumnPredicate) return false;

        return true;
    }
}
