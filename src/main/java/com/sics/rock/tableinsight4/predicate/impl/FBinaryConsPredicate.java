package com.sics.rock.tableinsight4.predicate.impl;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIConstantPredicate;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * constant predicate applying on 2 tuple
 * t0.age = 20 ^ t1.age = 20
 * <p>
 * This predicate is just combination of two individual unary constant predicates,
 * which have the same table, column and constant but tuple-id is 0 and 1 respectively.
 * To combine them eases the construction of rules,
 * because there are some restrictions in rules.
 *
 * @author zhaorx
 */
public class FBinaryConsPredicate implements FIConstantPredicate, FIBinaryPredicate {

    private final String tableName;
    private final String columnName;
    private final FOperator operator;
    private final FConstant<?> constant;
    private final Set<String> innerTabCols;


    public FBinaryConsPredicate(String tableName, String columnName, FOperator operator, FConstant<?> constant, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.operator = operator;
        this.constant = constant;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public String toString() {
        return toString("⋀", -1, false);
    }

    /**
     * ti.col op const ^ tj.col op const
     */
    public String toString(String syntaxConjunction, int maxDecimalPlace, boolean allowExponentialForm) {
        return String.format("t%d.%s %s %s %s t%d.%s %s %s",
                leftTableIndex(), columnName, operator.symbol, constant.toUserString(maxDecimalPlace, allowExponentialForm),
                syntaxConjunction,
                rightTableIndex(), columnName, operator.symbol, constant.toUserString(maxDecimalPlace, allowExponentialForm));
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
    public List<FConstant<?>> allConstants() {
        return Collections.singletonList(constant);
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
        return 2;
    }
}
