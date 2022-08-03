package com.sics.rock.tableinsight4.predicate.iface;

import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;

import java.util.List;


/**
 * constant predicate
 * e.g. c1 = "a"
 * e.g. c1 = "a" & c2 = "a"
 * e.g. c1 >= 2 & c1 < 5
 *
 * @author zhaorx
 */
public interface FIConstantPredicate extends FIPredicate {

    List<FConstant<?>> allConstants();

}
