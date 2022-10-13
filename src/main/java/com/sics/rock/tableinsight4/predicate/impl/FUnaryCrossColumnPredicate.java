package com.sics.rock.tableinsight4.predicate.impl;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;

import java.util.Set;

/**
 * single line cross column predicate
 * t0.xxx > t0.yyy
 *
 * @author zhaorx
 */
public class FUnaryCrossColumnPredicate implements FIUnaryPredicate {

    private final String tableName;
    private final String leftColumnName;
    private final String rightColumnName;
    private final int tupleIndex;
    private final FOperator operator;
    private final Set<String> innerTabCols;

    public FUnaryCrossColumnPredicate(String tableName, String leftColumnName, String rightColumnName,
                                      int tupleIndex, FOperator operator, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.tupleIndex = tupleIndex;
        this.operator = operator;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String columnName() {
        return leftColumnName;
    }

    public String leftColumnName() {
        return leftColumnName;
    }

    public String rightColumnName() {
        return rightColumnName;
    }

    @Override
    public int tupleIndex() {
        return tupleIndex;
    }

    @Override
    public FOperator operator() {
        return operator;
    }

    @Override
    public Set<String> innerTabCols() {
        return innerTabCols;
    }

    @Override
    public int length() {
        return 1;
    }

    @Override
    public String toString() {
        return "t" + tupleIndex + "." + leftColumnName + " " + operator.symbol + " t" + tupleIndex + "." + rightColumnName;
    }
}
