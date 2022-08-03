package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.conf.FTiConfiguring;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Constant value info about a column.
 * The values are all nullable
 *
 * @author zhaorx
 */
public class FConstantConfig extends FTiConfiguring implements Serializable {

    /**
     * Find null constant or not
     */
    public static final String CONFIG_FIND_NULL_CONSTANT = "findNullConstant";

    /**
     * The upper limit of ratio of appear-time of constant value
     * column.filter(_==constant).count / column.count <= constantRatioUpperLimit
     */
    public static final String CONFIG_UPPER_LIMIT_RATIO = "upperLimitRatio";

    /**
     * The down limit of ratio of appear-time of constant value
     * column.filter(_==constant).count / column.count >= constantRatioDownLimit
     */
    public static final String CONFIG_DOWN_LIMIT_RATIO = "downLimitRatio";

    /**
     * Find constant or not
     * case null: false
     */
    private final boolean findConstant;

    /**
     * External-imported interval constants
     * Defined by user or auto-generating
     * <p>
     * Do type cast if the type of element does not match the column value type
     * Support null value if the string is null or empty
     */
    private final ArrayList<String> externalConstants = new ArrayList<>();

    public FConstantConfig(boolean findConstant) {
        this.findConstant = findConstant;
    }

    public static FConstantConfig notFindConstantValue() {
        return new FConstantConfig(false);
    }

    public static FConstantConfig findConstantValue() {
        return new FConstantConfig(true);
    }

    public Boolean isFindConstant() {
        return findConstant;
    }

    public void addExternalConstants(String constant) {
        this.externalConstants.add(constant);
    }

    public ArrayList<String> getExternalConstants() {
        return externalConstants;
    }
}
