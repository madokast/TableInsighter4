package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.conf.FTiConfiguring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Interval constant info about a column.
 * The values are all nullable
 *
 * @author zhaorx
 */
public class FIntervalConstantConfig extends FTiConfiguring implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FIntervalConstantConfig.class);

    /**
     * Config key the left boundary is close or open
     */
    public static final String CONFIG_LEFT_CLOSE = "leftClose";

    /**
     * Config key the right boundary is close or open
     */
    public static final String CONFIG_RIGHT_CLOSE = "rightClose";

    /**
     * Config key for k-means parameter
     */
    public static final String CONFIG_K_MEANS_CLUSTER_NUMBER = "kMeans.clusterNumber";

    /**
     * Config key for k-means parameter
     */
    public static final String CONFIG_K_MEANS_ITER_NUMBER = "kMeans.iterNumber";


    /**
     * Find interval constant or not
     */
    private final boolean findIntervalConstant;

    /**
     * External-imported interval constants
     * Defined by user or auto-generating
     * <p>
     * The formats of externalIntervalConstant are listed below
     * 1. "a number". Create intervals (-Inf, num] and (num, +inf)
     * 2. "op number". Like ">5", ">=10", "≤20"
     * 3. "interval". Like "[3,5]", "(12,30]"
     */
    private final ArrayList<String> externalIntervalConstants = new ArrayList<>();

    private FIntervalConstantConfig(boolean findIntervalConstant) {
        this.findIntervalConstant = findIntervalConstant;
    }

    public static FIntervalConstantConfig notFindIntervalConstant() {
        return new FIntervalConstantConfig(false);
    }

    public static FIntervalConstantConfig findIntervalConstant() {
        return new FIntervalConstantConfig(true);
    }

    public void addExternalIntervalConstant(String externalIntervalConstant) {
        this.externalIntervalConstants.add(externalIntervalConstant);
    }

    public boolean isFindIntervalConstant() {
        return findIntervalConstant;
    }

    public ArrayList<String> getExternalIntervalConstants() {
        return externalIntervalConstants;
    }
}
