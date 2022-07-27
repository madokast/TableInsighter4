package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.predicate.iface.FIIntervalPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;

import java.util.Set;

/**
 * t0.age > 20
 * t0.age <= 20
 * t0.age in [20,30)
 *
 * @author zhaorx
 */
public class FUnaryIntervalConsPredicate implements FIIntervalPredicate, FIUnaryPredicate {

    private final String tableName;
    private final String columnName;
    private final int tupleId;
    private final FInterval interval;
    private final Set<String> innerTabCols;


    public FUnaryIntervalConsPredicate(String tableName, String columnName, int tupleId,
                                       FInterval interval, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tupleId = tupleId;
        this.interval = interval;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public FInterval interval() {
        return interval;
    }

    @Override
    public Set<String> innerTabCols() {
        return innerTabCols;
    }

    @Override
    public String toString() {
        return toString(-1, true);
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm) {
        return interval.inequalityOf("t" + tupleId + "." + columnName, maxDecimalPlace, allowExponentialForm);
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String columnName() {
        return columnName;
    }

    @Override
    public int tupleIndex() {
        return tupleId;
    }

    @Override
    public int length() {
        return (interval.left().isPresent() ? 1 : 0) + (interval.right().isPresent() ? 1 : 0);
    }
}
