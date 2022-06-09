package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FValueDefault;
import org.apache.spark.sql.catalyst.json.JSONOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Constant value info about a column.
 * The values are all nullable
 *
 * @author zhaorx
 */
public class FConstantConfig implements Serializable {

    /**
     * Find constant or not
     * case null: false
     */
    private final Boolean findConstant;

    /**
     * Find null constant or not
     * case null: false
     */
    private final Boolean findNullConstant;

    /**
     * The upper limit of ratio of appear-time of constant value
     * case null: back to upper config
     *
     * column.filter(_==constant).count / column.count <= constantRatioUpperLimit
     */
    private final Double constantRatioUpperLimit;

    /**
     * The down limit of ratio of appear-time of constant value
     * case null: back to upper config
     *
     * column.filter(_==constant).count / column.count >= constantRatioDownLimit
     */
    private final Double constantRatioDownLimit;

    /**
     * External-imported interval constants
     * Defined by user or auto-generating
     *
     * Do type cast if the type of element does not match the column value type
     * Support null value if the element is null or the element is a string and equals "null"
     */
    private final ArrayList<Object> externalConstants = new ArrayList<>();

    public FConstantConfig(Boolean findConstant, Boolean findNullConstant, Double constantRatioUpperLimit, Double constantRatioDownLimit) {
        this.findConstant = findConstant;
        this.findNullConstant = findNullConstant;
        this.constantRatioUpperLimit = constantRatioUpperLimit;
        this.constantRatioDownLimit = constantRatioDownLimit;
    }

    public static FConstantConfig notFindConstantValue() {
        return new FConstantConfig(false, null, null, null);
    }

    public static FConstantConfig findUsingDefault() {
        return new FConstantConfig(true, null, null, null);
    }

    public static FConstantConfig find(Boolean findNullConstant, Double constantRatioUpperLimit, Double constantRatioDownLimit) {
        return new FConstantConfig(true, findNullConstant, constantRatioUpperLimit, constantRatioDownLimit);
    }

    public Boolean isFindConstant(boolean configValue) {
        return FValueDefault.getOrDefault(findConstant, configValue);
    }

    public Boolean isFindNullConstant(boolean configValue) {
        return FValueDefault.getOrDefault(findNullConstant, configValue);
    }

    public Double getConstantRatioUpperLimit(double configValue) {
        return FValueDefault.getOrDefault(constantRatioUpperLimit, configValue);
    }

    public Double getConstantRatioDownLimit(double configValue) {
        return FValueDefault.getOrDefault(constantRatioDownLimit, configValue);
    }

    public void addExternalConstants(Object constant) {
        this.externalConstants.add(constant);
    }

    public ArrayList<Object> getExternalConstants() {
        return externalConstants;
    }
}
