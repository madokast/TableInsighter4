package com.sics.rock.tableinsight4.core.external;

/**
 * similar('levenshtein' , t0.ac, t1.ac, 0.85)
 * ML('ditto', t0.ac, t1.ac)
 *
 * @author zhaorx
 */
public interface FIExternalBianryModelPredicateNameFormatter {

    String format(int leftTupleId, int rightTupleId);

}
