package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.pli.FPliConstructor;
import com.sics.rock.tableinsight4.predicate.FPredicateFactory;
import com.sics.rock.tableinsight4.procedure.FConstantHandler;
import com.sics.rock.tableinsight4.procedure.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.procedure.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.procedure.FTableDataLoader;
import com.sics.rock.tableinsight4.procedure.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

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

    public /*rules*/ void findRule() {
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

            FPliConstructor pliConstructor = new FPliConstructor(config.idColumnName, config.sliceLengthForPLI, config.positiveNegativeExampleSwitch, spark);
            FPLI PLI = pliConstructor.construct(tableDatasetMap);

        } finally {
            FTiEnvironment.destroy();
        }
    }


    public FTableInsight(List<FTableInfo> tableInfos, List<FExternalBinaryModelInfo> externalBinaryModelInfos, FTiConfig config, SparkSession spark) {
        this.tableInfos = tableInfos;
        this.externalBinaryModelInfos = externalBinaryModelInfos;
        this.config = config;
        this.spark = spark;
    }
}
