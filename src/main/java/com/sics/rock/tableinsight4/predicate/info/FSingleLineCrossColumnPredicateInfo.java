package com.sics.rock.tableinsight4.predicate.info;

import com.sics.rock.tableinsight4.predicate.FIPredicate;

import java.util.Collections;
import java.util.List;

public class FSingleLineCrossColumnPredicateInfo implements FExternalPredicateInfo {


    @Override
    public List<FIPredicate> predicates() {
        return Collections.emptyList();
    }


}
