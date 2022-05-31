package com.sics.rock.tableinsight4.core.external;


/**
 * @author zhaorx
 */
public class FExternalBinaryModelInfo {

    private final String id;

    private final String leftTableName;

    private final String rightTableName;

    private final boolean target;

    private final FIExternalBinaryModelCalculator calculator;

    private final FIExternalBianryModelPredicateNameFormatter predicateNameFormatter;

    public FExternalBinaryModelInfo(String id, String leftTableName, String rightTableName, boolean target, FIExternalBinaryModelCalculator calculator, FIExternalBianryModelPredicateNameFormatter predicateNameFormatter) {
        this.id = id;
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
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

    public boolean isTarget() {
        return target;
    }

    public FIExternalBinaryModelCalculator getCalculator() {
        return calculator;
    }

    public FIExternalBianryModelPredicateNameFormatter getPredicateNameFormatter() {
        return predicateNameFormatter;
    }
}
