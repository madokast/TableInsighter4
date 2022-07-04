package com.sics.rock.tableinsight4.predicate.iface;


import com.sics.rock.tableinsight4.procedure.constant.FConstant;

import java.util.Optional;

/**
 * e.g. c1 >= 2 & c1 < 5
 *
 * @author zhaorx
 */
public interface FIIntervalPredicate extends FIUnaryPredicate, FIConstantPredicate {

    Optional<FConstant<Double>> left();

    Optional<FConstant<Double>> right();

    boolean leftClose();

    boolean rightClose();

}
