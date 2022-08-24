package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.evidenceset.factory.FEvidenceSetFactoryBuilder;
import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.factory.FSingleLineEvidenceSetFactory;
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
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
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

    public List<FRule> findRule() {
        List<FRule> allRules = new ArrayList<>();

        FTypeUtils.registerSparkUDF(spark.sqlContext());
        FTiEnvironment.create(spark, config);
        try {
            final FTableDataLoader dataLoader = new FTableDataLoader();
            final FTableDatasetMap tableDatasetMap = dataLoader.prepareData(tableInfos);

            FExternalBinaryModelHandler modelHandler = new FExternalBinaryModelHandler();
            modelHandler.appendDerivedColumn(tableDatasetMap, externalBinaryModelInfos);

            FIntervalsConstantHandler intervalsConstantHandler = new FIntervalsConstantHandler();
            intervalsConstantHandler.generateIntervalConstant(tableDatasetMap);

            FConstantHandler constantHandler = new FConstantHandler();
            constantHandler.generateConstant(tableDatasetMap);

            FPliConstructor pliConstructor = new FPliConstructorFactory().create();
            FPLI PLI = pliConstructor.construct(tableDatasetMap);

            FDerivedColumnNameHandler derivedColumnNameHandler = new FDerivedColumnNameHandler(externalBinaryModelInfos);

            List<FTableInfo> allTableInfos = tableDatasetMap.allTableInfos();
            allTableInfos.forEach(tableInfo -> {
                if (config.singleLineRuleFind) {   // single-line
                    FPredicateIndexer singleLinePredicateIndexer =
                            FPredicateFactory.createSingleLinePredicates(tableInfo, derivedColumnNameHandler, new ArrayList<>());
                    FSingleLineEvidenceSetFactory evidenceSetFactory = new FEvidenceSetFactoryBuilder().buildSingleLineEvidenceSetFactory();
                    FIEvidenceSet singleLineEvidenceSet = evidenceSetFactory.create(tableInfo, PLI,
                            singleLinePredicateIndexer, tableInfo.getLength(() -> tableDatasetMap.getDatasetByInnerTableName(tableInfo.getInnerTableName()).count()));
                    FIRuleFinder ruleFinder = new FRuleFinderBuilder().build(singleLinePredicateIndexer, Collections.singletonList(tableInfo), singleLineEvidenceSet);

                    List<FRule> rules = ruleFinder.find();
                    allRules.addAll(rules);
                }
            });

            for (int i = 0; i < allTableInfos.size(); i++) {
                FTableInfo leftTable = allTableInfos.get(i);
                for (int j = i + 1; j < allTableInfos.size(); j++) {
                    FTableInfo rightTable = allTableInfos.get(j);

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
