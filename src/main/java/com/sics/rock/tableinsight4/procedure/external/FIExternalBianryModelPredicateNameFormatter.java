package com.sics.rock.tableinsight4.procedure.external;

/**
 * similar('levenshtein' , t0.ac, t1.ac, 0.85)
 * ML('ditto', t0.ac, t1.ac)
 */
public interface FIExternalBianryModelPredicateNameFormatter {

    String format(int leftTupleId, int rightTupleId);

}
