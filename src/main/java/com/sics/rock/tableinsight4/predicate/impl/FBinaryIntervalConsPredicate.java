package com.sics.rock.tableinsight4.predicate.impl;

import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIIntervalPredicate;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;

import java.util.Set;

/**
 * interval predicate applying on 2 tuple
 * 20 < t0.age <= 40 ^ 20 < t1.age <= 40
 *
 * @author zhaorx
 * @see FBinaryConsPredicate
 */
public class FBinaryIntervalConsPredicate implements FIBinaryPredicate, FIIntervalPredicate {

    private final String tableName;
    private final String columnName;
    private final FInterval interval;
    private final Set<String> innerTabCols;


    public FBinaryIntervalConsPredicate(String tableName, String columnName, FInterval interval, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.interval = interval;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public String leftTableName() {
        return tableName;
    }

    @Override
    public String rightTableName() {
        return tableName;
    }

    @Override
    public String leftCol() {
        return columnName;
    }

    @Override
    public String rightCol() {
        return columnName;
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
    public FInterval interval() {
        return interval;
    }

    @Override
    public String toString() {
        return toString(-1, true, "⋀");
    }

    public String toString(int maxDecimalPlace, boolean allowExponentialForm, String syntaxConjunction) {
        return interval.inequalityOf("t0." + columnName, maxDecimalPlace, allowExponentialForm)
                + " " + syntaxConjunction + " "
                + interval.inequalityOf("t1." + columnName, maxDecimalPlace, allowExponentialForm);
    }

    @Override
    public Set<String> innerTabCols() {
        return innerTabCols;
    }

    @Override
    public int length() {
        return 2 * ((interval.left().isPresent() ? 1 : 0) + (interval.right().isPresent() ? 1 : 0));
    }
}
