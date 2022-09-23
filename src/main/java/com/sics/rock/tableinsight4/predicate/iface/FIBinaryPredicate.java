package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

/**
 * predicate applying 2 tuples
 *
 * @author zhaorx
 */
public interface FIBinaryPredicate extends FIPredicate {

    String leftTableName();

    String rightTableName();

    String leftCol();

    String rightCol();

    int leftTableIndex();

    int rightTableIndex();

}
