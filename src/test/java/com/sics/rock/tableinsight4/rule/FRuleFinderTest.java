package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.factory.FEvidenceSetFactoryBuilder;
import com.sics.rock.tableinsight4.evidenceset.factory.FSingleLineEvidenceSetFactory;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructorFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactoryBuilder;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FRuleFinderTest extends FTableInsightEnv {

    @Test
    public void find() {
        FTableInfo relation = FExamples.relation();
        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        new FExternalBinaryModelHandler().appendDerivedColumn(tableDatasetMap, Collections.emptyList());

        new FIntervalsConstantHandler().generateIntervalConstant(tableDatasetMap);

        new FConstantHandler().generateConstant(tableDatasetMap);

        FPLI PLI = new FPliConstructorFactory().create().construct(tableDatasetMap);

        FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(Collections.emptyList());

        tableDatasetMap.foreach((tabInfo, dataset) -> {
            FPredicateIndexer singleLinePredicateIndexer = new FPredicateFactoryBuilder(derivedColumnNameHandler, tableDatasetMap, PLI)
                    .buildForSingleLinePredicate()
                    .use(tabInfo, Collections.emptyList()).createPredicates();

            FSingleLineEvidenceSetFactory singleLineEvidenceSetFactory = new FEvidenceSetFactoryBuilder().buildSingleLineEvidenceSetFactory();

            FIEvidenceSet singleLineEvidenceSet = singleLineEvidenceSetFactory.create(
                    tabInfo, PLI, singleLinePredicateIndexer, tabInfo.getLength(dataset::count));

            FRuleFactory ruleFactory = new FRuleFactory(singleLinePredicateIndexer, Collections.singletonList(tabInfo), 20, config().tableColumnLinker);

            FRuleFinder ruleFinder = new FRuleFinder(ruleFactory, singleLineEvidenceSet, 0.0001,
                    0.8, 1000,
                    100000, 1000000,
                    100, 50);

            List<FRule> rules = ruleFinder.find();

            logger.info("rule number " + rules.size());

            rules.forEach(r -> logger.info("{}", r.toString(singleLinePredicateIndexer, "^", "->")));
        });

    }
}