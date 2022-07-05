package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;

import java.util.List;
import java.util.Set;

public class FIntervalConsPredicate implements FIConstantPredicate, FIUnaryPredicate {

    private final String tableName;
    private final String columnName;
    private final int tupleId;
    private final FInterval interval;
    private final Set<String> innerTabCols;


    public FIntervalConsPredicate(String tableName, String columnName, int tupleId,
                                  FInterval interval, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tupleId = tupleId;
        this.interval = interval;
        this.innerTabCols = innerTabCols;
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
        return interval.inequalityOf("t" + tupleId + "." + columnName);
    }

    @Override
    public List<FConstant<?>> allConstants() {
        return (List<FConstant<?>>) (List) interval.constants();
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
    public FOperator operator() {
        return FOperator.BELONG;
    }

    @Override
    public int length() {
        return 1;
    }
}
