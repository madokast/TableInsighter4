package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.range.FRangeInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhaorx
 */
public class FRangeDerivedColumnAppender {

    private static final Logger logger = LoggerFactory.getLogger(FRangeDerivedColumnAppender.class);

    private final String rangeDerivedColumnSuffix;

    // Key is tabName.colName
    private final Map<String, FRangeInfo> rangeInfoMap = new ConcurrentHashMap<>();

    private final SparkSession spark;


    private List<FRangeInfo> searchColumnRange(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        tableInfo.rangeColumns().parallelStream().forEach(columnInfo -> {

        });

        return null;
    }

    public FRangeDerivedColumnAppender(
            String rangeDerivedColumnSuffix, List<FRangeInfo> rangeInfos,
            SparkSession spark) {
        this.rangeDerivedColumnSuffix = rangeDerivedColumnSuffix;
        rangeInfos.forEach(info->rangeInfoMap.put(info.identifier(), info));
        this.spark = spark;
    }
}
