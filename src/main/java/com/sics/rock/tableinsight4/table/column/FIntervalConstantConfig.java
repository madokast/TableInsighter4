package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FValueDefault;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Interval constant info about a column.
 * The values are all nullable
 *
 * @author zhaorx
 */
public class FIntervalConstantConfig implements Serializable {

    /**
     * Find interval constant or not
     * case null: false
     */
    private final Boolean findIntervalConstant;

    /**
     * K-means parameter
     * <p>
     * case null: use the default value in environment config
     */
    private final Integer kMeansClusterNumber;

    /**
     * K-means parameter
     * <p>
     * case null: use the default value in environment config
     */
    private final Integer kMeansIterNumber;

    /**
     * The left boundary is close or open
     * case null: use the default value in environment config
     */
    private final Boolean leftClose;

    /**
     * The right boundary is close or open
     * case null: use the default value in environment config
     */
    private final Boolean rightClose;

    /**
     * External-imported interval constants
     * Defined by user or auto-generating
     *
     * The formats of externalIntervalConstant are listed below
     * 1. "a number". Create intervals (-Inf, num] and (num, +inf)
     * 2. "op number". Like ">5", ">=10", "≤20"
     * 3. "interval". Like "[3,5]", "(12,30]"
     */
    private final ArrayList<String> externalIntervalConstants = new ArrayList<>();

    private FIntervalConstantConfig(Boolean findIntervalConstant, Integer kMeansClusterNumber, Integer kMeansIterNumber, Boolean leftClose, Boolean rightClose) {
        this.findIntervalConstant = findIntervalConstant;
        this.kMeansClusterNumber = kMeansClusterNumber;
        this.kMeansIterNumber = kMeansIterNumber;
        this.leftClose = leftClose;
        this.rightClose = rightClose;
    }

    public static FIntervalConstantConfig notFindIntervalConstant() {
        return new FIntervalConstantConfig(false, null, null, null, null);
    }

    public static FIntervalConstantConfig findUsingDefaultConfig() {
        return new FIntervalConstantConfig(true, null, null, null, null);
    }

    public static FIntervalConstantConfig find(Integer clusterNumber, Integer iterNumber, Boolean leftClose, Boolean rightClose) {
        return new FIntervalConstantConfig(true, clusterNumber, iterNumber, leftClose, rightClose);
    }

    public void addExternalIntervalConstant(String externalIntervalConstant) {
        this.externalIntervalConstants.add(externalIntervalConstant);
    }

    public boolean isFindIntervalConstant() {
        return FValueDefault.getOrDefault(findIntervalConstant, false);
    }

    public int getClusterNumber(int configValue) {
        return FValueDefault.getOrDefault(kMeansClusterNumber, configValue);
    }

    public int getIterNumber(int configValue) {
        return FValueDefault.getOrDefault(kMeansIterNumber, configValue);
    }

    public boolean getLeftClose(boolean configValue) {
        return FValueDefault.getOrDefault(leftClose, configValue);
    }

    public boolean getRightClose(boolean configValue) {
        return FValueDefault.getOrDefault(rightClose, configValue);
    }

    public ArrayList<String> getExternalIntervalConstants() {
        return externalIntervalConstants;
    }
}
