package com.sics.rock.tableinsight4.predicate.iface;

/**
 * similar('levenshtein' , t0.ac, t1.ac, 0.85)
 * ML('ditto', t0.ac, t1.ac)
 *
 * @author zhaorx
 */
public interface FIBinaryModelPredicate extends FIBinaryPredicate {

    String toString(int leftTupleId, int rightTupleId);

}
