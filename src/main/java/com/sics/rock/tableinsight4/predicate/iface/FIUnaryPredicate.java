package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

/**
 * single-line predicate
 *
 * @author zhaorx
 */
public interface FIUnaryPredicate extends FIPredicate {

    String tableName();

    String columnName();

    int tupleIndex();
}
