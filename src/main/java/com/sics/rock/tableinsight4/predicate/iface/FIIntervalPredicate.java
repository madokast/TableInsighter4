package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;

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
    default List<FConstant<?>> allConstants() {
        return interval().constants();
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
