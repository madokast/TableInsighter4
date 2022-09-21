package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.factory.FEvidenceSetFactoryBuilder;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.pli.FPliConstructorFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.preprocessing.FConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.preprocessing.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.preprocessing.FTableDataLoader;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.rule.FIRuleFinder;
import com.sics.rock.tableinsight4.rule.FRule;
import com.sics.rock.tableinsight4.rule.FRuleFinderBuilder;
import com.sics.rock.tableinsight4.rule.FRuleVO;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Entry
 *
 * @author zhaorx
 */
public class FTableInsight {

    private static final Logger logger = LoggerFactory.getLogger(FTableInsight.class);

    private final List<FTableInfo> tableInfos;

    private final List<FExternalBinaryModelInfo> externalBinaryModelInfos;

    private final FTiConfig config;

    private final SparkSession spark;

    public List<FRuleVO> findRule() {
        final List<FRuleVO> allRules = new ArrayList<>();

        FTypeUtils.registerSparkUDF(spark.sqlContext());
        FTiEnvironment.create(spark, config);
        try {
            final FTableDataLoader dataLoader = new FTableDataLoader();
            final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(tableInfos);

            final FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
            modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

            final FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
            intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

            final FConstantHandler constantHandler = new FConstantHandler();
            constantHandler.generateConstant(tableDatasetMap);

            final FPliConstructor pliConstructor = new FPliConstructorFactory().create();
            final FPLI PLI = pliConstructor.construct(tableDatasetMap);

            final FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

            final List<FTableInfo> allTableInfos = tableDatasetMap.allTableInfos();

            allTableInfos.forEach(tableInfo -> {
                if (config.singleLineRuleFind) {// single-line
                    final FPredicateIndexer predicates =
                            FPredicateFactory.createSingleLinePredicates(tableInfo, derivedColumnNameHandler, new ArrayList<>());
                    final FIEvidenceSet evidenceSet = new FEvidenceSetFactoryBuilder()
                            .buildSingleLineEvidenceSetFactory().create(tableInfo, PLI, predicates,
                                    tableInfo.getLength(() -> tableDatasetMap.getDatasetByInnerTableName(tableInfo.getInnerTableName()).count()));
                    final FIRuleFinder ruleFinder = new FRuleFinderBuilder().build(predicates, Collections.singletonList(tableInfo), evidenceSet);

                    final List<FRule> rules = ruleFinder.find();
                    allRules.addAll(FRuleVO.create(rules, PLI, evidenceSet, predicates, Collections.singletonList(tableInfo),
                            config.syntaxConjunction, config.syntaxImplication, config.positiveNegativeExampleNumber));
                }

                if (config.singleTableCrossLineRuleFind) {// single-table-cross-line
                    final FPredicateIndexer predicates =
                            FPredicateFactory.createSingleTableCrossLinePredicates(tableInfo, config.constPredicateCrossLine, derivedColumnNameHandler, new ArrayList<>());
                    final FIEvidenceSet evidenceSet = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory()
                            .createSingleTableBinaryLineEvidenceSet(tableInfo, PLI, predicates,
                                    tableInfo.getLength(() -> tableDatasetMap.getDatasetByInnerTableName(tableInfo.getInnerTableName()).count()));
                    final FIRuleFinder ruleFinder = new FRuleFinderBuilder().build(predicates,
                            Collections.singletonList(tableInfo), evidenceSet);

                    final List<FRule> rules = ruleFinder.find();
                    allRules.addAll(FRuleVO.create(rules, PLI, evidenceSet, predicates, FTiUtils.listOf(tableInfo, tableInfo),
                            config.syntaxConjunction, config.syntaxImplication, config.positiveNegativeExampleNumber));

                }
            });

            if (config.crossTableCrossLineRuleFind) {
                for (int i = 0; i < allTableInfos.size(); i++) {
                    final FTableInfo leftTable = allTableInfos.get(i);
                    for (int j = i + 1; j < allTableInfos.size(); j++) {
                        final FTableInfo rightTable = allTableInfos.get(j);
                        final FPredicateIndexer predicates = FPredicateFactory.createMultiTableCrossLinePredicates(
                                leftTable, rightTable, derivedColumnNameHandler, PLI, tableDatasetMap,
                                config.crossColumnThreshold, Collections.emptyList());
                        final FIEvidenceSet evidenceSet = new FEvidenceSetFactoryBuilder().buildBinaryLineEvidenceSetFactory()
                                .createBinaryTableBinaryLineEvidenceSet(leftTable, rightTable, PLI, predicates,
                                        leftTable.getLength(() -> tableDatasetMap.getDatasetByInnerTableName(leftTable.getInnerTableName()).count()),
                                        rightTable.getLength(() -> tableDatasetMap.getDatasetByInnerTableName(rightTable.getInnerTableName()).count()));
                        final FIRuleFinder ruleFinder = new FRuleFinderBuilder().build(predicates, FTiUtils.listOf(leftTable, rightTable), evidenceSet);
                        final List<FRule> rules = ruleFinder.find();

                        allRules.addAll(FRuleVO.create(rules, PLI, evidenceSet, predicates,
                                FTiUtils.listOf(leftTable, rightTable),
                                config.syntaxConjunction, config.syntaxImplication, config.positiveNegativeExampleNumber));
                    }
                }
            }


        } finally {
            FTiEnvironment.destroy();
        }

        return allRules;
    }


    public FTableInsight(List<FTableInfo> tableInfos, List<FExternalBinaryModelInfo> externalBinaryModelInfos,
                         FTiConfig config, SparkSession spark) {
        this.tableInfos = tableInfos;
        this.externalBinaryModelInfos = externalBinaryModelInfos;
        this.config = config;
        this.spark = spark;
    }
}
