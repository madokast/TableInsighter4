package com.sics.rock.tableinsight4.evidenceset.predicateset;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.FPredicateFactory;

import java.io.Serializable;


/**
 * Used only in evidence set
 *
 * @author zhaorx
 */
public interface FIPredicateSet extends Serializable {

    FBitSet getBitSet();

    long getSupport();

    String toString(FPredicateFactory indexProvider);
}
