package com.sics.rock.tableinsight4.procedure.external.binary;


import java.util.List;

/**
 * model applying on 2 tuples
 *
 * @author zhaorx
 */
public class FExternalBinaryModelInfo {

    private final String id;

    private final String leftTableName;

    private final String rightTableName;

    /**
     * for predicate compatibility and toString
     */
    private final List<String> leftColumns;

    /**
     * for predicate compatibility and toString
     */
    private final List<String> rightColumns;

    private final boolean target;

    private final FIExternalBinaryModelCalculator calculator;

    private final FIExternalBinaryModelPredicateNameFormatter predicateNameFormatter;

    public FExternalBinaryModelInfo(String id, String leftTableName, String rightTableName,
                                    List<String> leftColumns, List<String> rightColumns,
                                    boolean target, FIExternalBinaryModelCalculator calculator,
                                    FIExternalBinaryModelPredicateNameFormatter predicateNameFormatter) {
        this.id = id;
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        this.target = target;
        this.calculator = calculator;
        this.predicateNameFormatter = predicateNameFormatter;
    }

    public String getId() {
        return id;
    }

    public String getLeftTableName() {
        return leftTableName;
    }

    public String getRightTableName() {
        return rightTableName;
    }

    public List<String> getLeftColumns() {
        return leftColumns;
    }

    public List<String> getRightColumns() {
        return rightColumns;
    }

    public boolean isTarget() {
        return target;
    }

    public FIExternalBinaryModelCalculator getCalculator() {
        return calculator;
    }

    public FIExternalBinaryModelPredicateNameFormatter getPredicateNameFormatter() {
        return predicateNameFormatter;
    }

    @Override
    public String toString() {
        return "FExternalBinaryModelInfo{" + id + '}';
    }
}
