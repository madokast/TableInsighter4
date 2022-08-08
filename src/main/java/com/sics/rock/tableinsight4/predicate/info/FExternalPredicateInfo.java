package com.sics.rock.tableinsight4.predicate.info;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

import java.util.ArrayList;
import java.util.List;

/**
 * other predicate used in rule find
 * 1. single-line cross-column predicate: t0.day1 > t0.day2
 *
 * @author zhaorx
 */
public interface FExternalPredicateInfo {

    List<FIPredicate> predicates();

    default FExternalPredicateInfo and(FExternalPredicateInfo another) {
        return () -> {
            List<FIPredicate> allPredicates = new ArrayList<>();
            allPredicates.addAll(this.predicates());
            allPredicates.addAll(another.predicates());
            return allPredicates;
        };
    }

}
