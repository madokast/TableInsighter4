package com.sics.rock.tableinsight4.predicate;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * A predicate is a logic element in a rule.
 * TI system supports 3 kinds of predicates:
 * 1. constant single-line predicate. Like "t0.age = 20", "t1.age > 20"
 * 2. constant two-line predicate. Like "t0.age = 20  ^ t1.age = 20". It only arrears in LHS
 * 3. normal (non-constant) two-line predicate. Like t0.age = t1.age、ML predicate、similar predicate
 * <p>
 * The compatibility between predicates
 * The compatibility between predicates determines if two predicates can appear in one rule.
 * If two predicates share same table-column, they are not comparable.
 * <p>
 * The constant predicates in multi-line rules.
 * In order to reduce the number of rules, these are some restrictions about multi-line rules.
 * 1. At least one normal two-line predicate is required.
 * 　The rule "t0.age = 30 ^ t0.height = 170 ^ t1.height = 170 -> t1.age = 30" violates it.
 * 2. The constant predicates in one multi-line rule should appear in pairs.
 * 　① t0.age = 20 ^ X -> t1.age = 20
 * 　② t0.age = 20 ^ t1.age = 20 ^ X -> Y
 * 　That's why "constant two-line predicate" is defined alone.
 *
 * @author zhaorx
 */
public interface FIPredicate extends Serializable {

    /**
     * logic operator
     */
    FOperator operator();

    /**
     * identifier
     *
     * relative table column of this predicate
     * for compatibility between predicates
     *
     * Derived column names should revert to origin ones
     */
    Set<String> innerTabCols();

    /**
     * length of this predicate
     * <p>
     * t0.age = 30             length = 1
     * t0.age = t1.age         length = 1
     * t0.age = 1 ^ t1.age = 1 length = 2
     */
    int length();
}
