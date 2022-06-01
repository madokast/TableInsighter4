package com.sics.rock.tableinsight4.table.column;

import java.io.Serializable;

/**
 * Info about a column finds range constant or not.
 * The ranges are found bu k-means FKMeansUtils
 *
 * @author zhaorx
 */
public class FValueRangeConfig implements Serializable {

    /**
     * Find range constant or not
     */
    private boolean findRangeConstant;

    /**
     * K-means parameter
     * <p>
     * case 0: use the default value in environment config
     */
    private int clusterNumber;

    /**
     * K-means parameter
     * <p>
     * case 0: use the default value in environment config
     */
    private int iterNumber;

    private FValueRangeConfig(boolean findRangeConstant, int clusterNumber, int iterNumber) {
        this.findRangeConstant = findRangeConstant;
        this.clusterNumber = clusterNumber;
        this.iterNumber = iterNumber;
    }

    public static FValueRangeConfig notFind() {
        return new FValueRangeConfig(false, 0, 0);
    }

    public static FValueRangeConfig findWithDefaultConfig() {
        return new FValueRangeConfig(true, 0, 0);
    }

    public static FValueRangeConfig find(int clusterNumber, int iterNumber) {
        return new FValueRangeConfig(true, clusterNumber, iterNumber);
    }

    public boolean isFindRangeConstant() {
        return findRangeConstant;
    }

    public int getClusterNumber(int configValue) {
        return clusterNumber == 0 ? configValue : clusterNumber;
    }

    public int getIterNumber(int configValue) {
        return iterNumber == 0 ? configValue : clusterNumber;
    }
}
