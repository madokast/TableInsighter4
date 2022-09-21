package com.sics.rock.tableinsight4.predicate.info;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

import java.util.Collections;
import java.util.List;

/**
 * precursor of single-line cross-column predicate
 *
 * @author zhaorx
 */
public class FSingleLineCrossColumnPredicateInfo implements FExternalPredicateInfo {

    @Override
    public List<FIPredicate> predicates() {
        return Collections.emptyList();
    }


}
