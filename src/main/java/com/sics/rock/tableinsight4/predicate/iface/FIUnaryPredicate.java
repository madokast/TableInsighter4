package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

public interface FIUnaryPredicate extends FIPredicate {

    String tableName();

    String columnName();

    int tupleIndex();
}
