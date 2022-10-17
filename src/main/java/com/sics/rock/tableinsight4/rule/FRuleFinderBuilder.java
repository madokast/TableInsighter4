package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.table.FTableInfo;

import java.util.List;

/**
 * rule finder builder
 *
 * @author zhaorx
 */
public class FRuleFinderBuilder implements FTiEnvironment {

    public FIRuleFinder build(FPredicateIndexer predicateIndexer, List<FTableInfo> tableInfos, FIEvidenceSet evidenceSet) {
        int maxRuleLength = config().maxRuleLength > 2 ? config().maxRuleLength : 2;
        FRuleFactory ruleFactory = new FRuleFactory(predicateIndexer, tableInfos, maxRuleLength, config().tableColumnLinker);


        return new FRuleFinder(ruleFactory, evidenceSet, config().cover, config().confidence,
                config().maxRuleNumber, config().maxNeedCreateChildrenRuleNumber,
                config().ruleFindTimeoutMinute * 60 * 1000, config().ruleFindBatchSizeMB,
                config().ruleFindMaxBatchNumber);
    }

}
