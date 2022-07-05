package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.internal.FIndexProvider;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.predicate.iface.FIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * predicate factory
 *
 * @author zhaorx
 */
public class FPredicateFactory {

    private static final Logger logger = LoggerFactory.getLogger(FPredicateFactory.class);

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

    /**
     * support of each predicate
     */
    private long[] supportMap;

    /**
     * create single-line rules like t0.name = aaa
     */
    public static FPredicateFactory createSingleLinePredicateFactory(
            FTableInfo table, FDerivedColumnNameHandler derivedColumnNameHandler) {
        final FPredicateFactory factory = new FPredicateFactory();

        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName,columnName);
            columnInfo.getConstants().stream().map(cons ->
                    new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols)
            ).forEach(factory::put);
            columnInfo.getIntervalConstants().stream().map(interval ->
                    new FIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols)
            ).forEach(factory::put);
        });

        return factory;
    }

    public static FPredicateFactory createSingleTableCrossLinePredicateFactory(
            FTableInfo table, boolean constantPredicate, FDerivedColumnNameHandler derivedColumnNameHandler) {
        FPredicateFactory factory = new FPredicateFactory();
        String tabName = table.getTableName();
        String innerTableName = table.getInnerTableName();


        return factory;
    }


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

    private void put(FIPredicate predicate) {
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

    public void setSupportMap(long[] supportMap) {
        this.supportMap = supportMap;
    }

    public long support(int predicateId) {
        if (supportMap != null) return supportMap[predicateId];
        else return 0L;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    private FPredicateFactory() {
    }
}
