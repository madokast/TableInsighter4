package com.sics.rock.tableinsight4.predicate.impl;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;

import java.util.Set;

/**
 * binary-line predicate
 * t0.age = t1.age
 *
 * @author zhaorx
 */
public class FBinaryPredicate implements FIBinaryPredicate {

    private final String leftTableName;
    private final String rightTableName;
    private final String leftColumnName;
    private final String rightColumnName;
    private final FOperator operator;
    private final Set<String> innerTabCols;

    public FBinaryPredicate(String leftTableName, String rightTableName, String leftColumnName,
                            String rightColumnName, FOperator operator, Set<String> innerTabCols) {
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.operator = operator;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public String toString() {
        return "t0." + leftColumnName + " " + operator.symbol + " t1." + rightColumnName;
    }

    @Override
    public String leftTableName() {
        return leftTableName;
    }

    @Override
    public String rightTableName() {
        return rightTableName;
    }

    @Override
    public String leftCol() {
        return leftColumnName;
    }

    @Override
    public String rightCol() {
        return rightColumnName;
    }

    @Override
    public int leftTableIndex() {
        return 0;
    }

    @Override
    public int rightTableIndex() {
        return 1;
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
}
