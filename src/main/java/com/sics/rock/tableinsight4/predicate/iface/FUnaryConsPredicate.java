package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FUnaryConsPredicate implements FIConstantPredicate, FIUnaryPredicate {

    private final String tableName;
    private final String columnName;
    private final int tupleId;
    private final FOperator operator;
    private final FConstant<?> constant;
    private final Set<String> innerTabCols;


    public FUnaryConsPredicate(String tableName, String columnName, int tupleId,
                               FOperator operator, FConstant<?> constant, Set<String> innerTabCols) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.tupleId = tupleId;
        this.operator = operator;
        this.constant = constant;
        this.innerTabCols = innerTabCols;
    }

    @Override
    public Set<String> innerTabCols() {
        return innerTabCols;
    }

    @Override
    public String toString() {
        return "t" + tupleId + "." + columnName + " " + operator.symbol + " '" + constant.getConstant() + "'";
    }

    @Override
    public List<FConstant<?>> allConstants() {
        return Collections.singletonList(constant);
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
        return operator;
    }

    @Override
    public int length() {
        return 1;
    }
}
