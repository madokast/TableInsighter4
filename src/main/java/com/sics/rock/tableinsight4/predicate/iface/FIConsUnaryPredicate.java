package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.core.constant.FConstant;

public interface FIConsUnaryPredicate extends FIConstantPredicate, FIUnaryPredicate {

    FConstant<?> constant();

}
