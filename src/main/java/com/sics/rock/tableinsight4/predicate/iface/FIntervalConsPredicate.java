package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;

import java.util.Collections;
import java.util.List;

public class FIntervalConsPredicate implements FIConstantPredicate, FIUnaryPredicate {

    private final String tableName;
    private final String columnName;
    private final int tupleId;
    private final FInterval interval;
    private final String innerTabCol;


    public FIntervalConsPredicate(String tableName, String columnName, int tupleId,
                                  FInterval interval, String innerTabCol) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tupleId = tupleId;
        this.interval = interval;
        this.innerTabCol = innerTabCol;
    }

    @Override
    public List<String> innerTabCols() {
        return Collections.singletonList(innerTabCol);
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
