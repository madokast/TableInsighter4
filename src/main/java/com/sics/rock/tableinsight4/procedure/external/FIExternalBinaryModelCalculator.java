package com.sics.rock.tableinsight4.procedure.external;

import com.sics.rock.tableinsight4.internal.FPair;

import java.util.List;

/**
 * External row paris calculator
 * Each pair records the row k and v are the same in the external model.
 *
 * @author zhaorx
 */
public interface FIExternalBinaryModelCalculator {

    /**
     * Get all result.
     * The pairs are used to build union-find set and generate classification table.
     * The process conducts in a single node (driver).
     * Waiting optimization...
     */
    List<FPair<Long, Long>> calculate();

}
