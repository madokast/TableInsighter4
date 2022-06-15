package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.core.FConstantHandler;
import com.sics.rock.tableinsight4.core.FExternalBinaryModelHandler;
import com.sics.rock.tableinsight4.core.FIntervalsConstantHandler;
import com.sics.rock.tableinsight4.core.FTableDataLoader;
import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * Entry
 *
 * @author zhaorx
 */
public class FTableInsight {

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
