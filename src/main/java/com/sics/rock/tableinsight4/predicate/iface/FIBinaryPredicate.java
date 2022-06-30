package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

public interface FIBinaryPredicate extends FIPredicate {

    String leftTable();

    String rightTable();

    String leftCol();

    String rightCol();

    int leftTableIndex();

    int rightTableIndex();

}
