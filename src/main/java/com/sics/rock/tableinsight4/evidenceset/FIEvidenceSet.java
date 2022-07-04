package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.rule.FRule;

import java.util.List;

public interface FIEvidenceSet {

    List<FRule> applyOn(List<FRule> rules);

    long allCount();

    long[] predicateSupport();
}

