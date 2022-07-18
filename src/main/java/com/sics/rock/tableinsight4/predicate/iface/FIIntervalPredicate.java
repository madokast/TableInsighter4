package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;

import java.util.List;
import java.util.Set;

/**
 * Interval predicate is also constant predicate
 *
 * @author zhaorx
 */
public interface FIIntervalPredicate extends FIConstantPredicate {

    FInterval interval();

    @Override
    @SuppressWarnings("unchecked")
    default List<FConstant<?>> allConstants() {
        return (List<FConstant<?>>) (List) interval().constants();
    }

    @Override
    default FOperator operator() {
        return FOperator.BELONG;
    }

    @Override
    Set<String> innerTabCols();

    @Override
    int length();

}
