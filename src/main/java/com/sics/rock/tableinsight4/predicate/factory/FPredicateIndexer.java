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

    FPredicateIndexer() {
    }

    /**
     * find predicate by feature
     * debug only
     */
    public List<FIPredicate> find(String feature) {
        return this.allPredicates().stream().filter(p ->
                feature.chars().mapToObj(ch -> String.valueOf((char) ch)).allMatch(ch -> p.toString().contains(ch))
        ).collect(Collectors.toList());
    }

    /**
     * find predicate index by feature
     * debug only
     */
    public List<Integer> findIndex(String feature) {
        return find(feature).stream().map(this::getIndex).collect(Collectors.toList());
    }

}
