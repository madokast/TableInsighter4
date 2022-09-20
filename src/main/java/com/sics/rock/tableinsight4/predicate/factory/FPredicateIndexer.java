package com.sics.rock.tableinsight4.predicate.factory;

import com.sics.rock.tableinsight4.internal.FIndexProvider;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * a predicate set used in one rule finding
 * the main function of this class is providing a indexer of predicate
 *
 * @author zhaorx
 */
public class FPredicateIndexer implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FPredicateIndexer.class);

    private final FIndexProvider<FIPredicate> indexProvider = new FIndexProvider<>();

    /**
     * key: consBiPredicate likes t0.name = aaa ^ t1.name = aaa
     * value: divided consUnaryPredicates from key, like t0.name = aaa and t1.name = aaa
     * all the predicate are represented in index
     */
    private final Map<Integer, FPair<Integer, Integer>> constBiPred2DivisionPred = new HashMap<>();

    /**
     * key: const-val predicate whose tuple-id is 0, like t0.name = aaa
     * value: the counterpart const-val predicate whose tuple-id is 1, like t1.name = aaa
     */
    private final Map<Integer, Integer> constPredT01Map = new HashMap<>();

    /**
     * the inverse map of constPredT01Map
     */
    private final Map<Integer, Integer> constPredT10Map = new HashMap<>();


    public int getIndex(final FIPredicate predicate) {
        FAssertUtils.require(() -> indexProvider.contain(predicate),
                () -> "Element " + predicate + " is not in indexProvider " + indexProvider);
        return indexProvider.getIndex(predicate);
    }

    public FIPredicate getPredicate(final int index) {
        FIPredicate predicate = indexProvider.get(index);
        FAssertUtils.require(() -> predicate != null,
                () -> "No element of index" + index + " in indexProvider = " + indexProvider);
        return predicate;
    }

    public List<FIPredicate> allPredicates() {
        return indexProvider.getAll();
    }

    public int size() {
        return indexProvider.size();
    }

    void put(FIPredicate predicate) {
        FAssertUtils.require(() -> !indexProvider.contain(predicate),
                () -> "Duplicate element " + predicate + " put in indexProvider" + indexProvider);
        indexProvider.put(predicate);
        logger.info("Create predicate NO.{} {}", getIndex(predicate), predicate);
    }

    void insertConstPredT01Map(FIPredicate t0ConstPredicate, FIPredicate t1ConstPredicate) {
        constPredT01Map.put(getIndex(t0ConstPredicate), getIndex(t1ConstPredicate));
        constPredT10Map.put(getIndex(t1ConstPredicate), getIndex(t0ConstPredicate));
    }

    void insertConstBiPred2DivisionPred(FIPredicate constBinaryPredicate,
                                        FIPredicate t0ConstPredicate, FIPredicate t1ConstPredicate) {
        constBiPred2DivisionPred.put(getIndex(constBinaryPredicate), new FPair<>(getIndex(t0ConstPredicate), getIndex(t1ConstPredicate)));
    }

    /**
     * divided consUnaryPredicates of consBiPredicate
     * input: like t0.name = aaa ^ t1.name = aaa
     * output: like t0.name = aaa and t1.name = aaa
     */
    public FPair<Integer, Integer> getDivideConsPred(int constBiPredIndex) {
        return constBiPred2DivisionPred.get(constBiPredIndex);
    }

    /**
     * all unary constant predicate
     * like t0.name = aaa
     * like t1.name = aaa
     * like t0.age in [20, 30]
     */
    public List<Integer> allUnaryConstantPredicates() {
        return allPredicates().stream().filter(p -> p instanceof FUnaryConsPredicate)
                .map(this::getIndex)
                .collect(Collectors.toList());
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * created by predicate factory
     */
    FPredicateIndexer() {
    }

    /**
     * find predicate by keywords
     * debug only
     */
    public List<FIPredicate> find(String keywords) {
        final char[] keywordArr = keywords.toCharArray();
        return this.allPredicates().stream().filter(p -> {
            final String pStr = p.toString();
            int start = 0;
            for (final char f : keywordArr) {
                final int id = pStr.indexOf(f, start);
                if (id < 0) return false;
                else start = id + 1;
            }
            return start > 0;
        }).collect(Collectors.toList());
    }

    /**
     * find predicate index by keywords
     * debug only
     */
    public List<Integer> findIndex(String keywords) {
        return find(keywords).stream().map(this::getIndex).collect(Collectors.toList());
    }

    public int getUnaryPredicateT0ByT1(int index) {
        FAssertUtils.require(() -> constPredT10Map.containsKey(index),
                () -> "constPredT10Map does not contain key " + getPredicate(index));
        return this.constPredT10Map.get(index);
    }
}
