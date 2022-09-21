package com.sics.rock.tableinsight4.predicate.impl;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

import java.util.Set;

/**
 * The binary model predicate is a binary predicate except overriding the toString method
 *
 * @author zhaorx
 */
public class FBinaryModelPredicate extends FBinaryPredicate {

    public FBinaryModelPredicate(String leftTableName, String rightTableName,
                                 String derivedColumnName, Set<String> innerTabCols) {
        super(leftTableName, rightTableName, derivedColumnName, derivedColumnName, FOperator.EQ, innerTabCols);
    }

    public String toString(FDerivedColumnNameHandler derivedColumnNameHandler) {
        String derivedModelColumnName = leftCol();
        return derivedColumnNameHandler.extractModelInfo(derivedModelColumnName)
                .getPredicateNameFormatter().format(leftTableIndex(), rightTableIndex());
    }
}
