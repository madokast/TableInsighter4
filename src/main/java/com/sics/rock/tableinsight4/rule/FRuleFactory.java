package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FRuleFactory {

    private static final Logger logger = LoggerFactory.getLogger(FRuleFactory.class);

    private final FPredicateIndexer predicateIndexer;

    /**
     * total predicate number
     * predicateIndexer.size
     */
    private final int predicateSize;

    /**
     * max number of left hand side predicates
     */
    private final int maxLhsSize;

    /**
     * is there multi-line (binary) predicate in predicateIndexer
     */
    private final boolean multiLinePredicateExistence;

    /**
     * predicates that are able to be use at right hand side
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
     * all predicates are addable when single line rule find.
     * binary predicates are addable when multi-line (2 lines) rule find.
     * <p>
     * when finding, a predicate can add to a rule iff. addable & compatible & under size limit
     * <p>
     * these predicates are record in a bitset for fast check
     */
    private final FBitSet addablePredicates;



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
            // in multi line rule find, single line const / interval predicate with tuple-id 0 cannot be rhs
            if (multiLinePredicateExistence && predicate instanceof FIUnaryPredicate) {
                if (((FIUnaryPredicate) predicate).tupleIndex() == 0) {
                    logger.info("Predicate {} cannot be used at right hand side", predicate);
                    return false;
                }
            }

            // binary const / interval predicate cannot be rhs
            if (predicate instanceof FBinaryConsPredicate) {
                logger.info("Predicate {} cannot be used at right hand side", predicate);
                return false;
            }

            if (predicate instanceof FBinaryIntervalConsPredicate) {
                logger.info("Predicate {} cannot be used at right hand side", predicate);
                return false;
            }

            // predicate cannot be rhs, if its relative column is non target
            for (String identifier : predicate.innerTabCols()) {
                if (nonTargets.contains(identifier)) {
                    logger.info("Predicate {} cannot be used at right hand side", predicate);
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
}
