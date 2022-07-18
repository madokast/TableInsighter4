package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

import java.util.Set;

/**
 * The binary model predicate is a binary predicate except overriding the toString method
 *
 */
public class FBinaryModelPredicate extends FBinaryPredicate {

    private final String modelPredicateString;

    public FBinaryModelPredicate(String leftTableName, String rightTableName,
                                 String derivedColumnName, Set<String> innerTabCols,
                                 String modelPredicateString) {
        super(leftTableName, rightTableName, derivedColumnName, derivedColumnName, FOperator.EQ, innerTabCols);
        this.modelPredicateString = modelPredicateString;
    }

    @Override
    public String toString() {
        return modelPredicateString;
    }

    public String toString(FDerivedColumnNameHandler derivedColumnNameHandler) {
        String derivedModelColumnName = leftCol();
        return derivedColumnNameHandler.extractModelInfo(derivedModelColumnName)
                .getPredicateNameFormatter().format(leftTableIndex(), rightTableIndex());
    }
}
