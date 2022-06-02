package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.range.FColumnValueRange;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author zhaorx
 */
public class FRangeDerivedColumnAppender {

    private static final Logger logger = LoggerFactory.getLogger(FRangeDerivedColumnAppender.class);

    private final String rangeDerivedColumnSuffix;

    private final List<FColumnValueRange> rangeInfos;

    private final SparkSession spark;

    public void appendRangeDerivedColumn(FTableDatasetMap tableDatasetMap) {
        // TODO
    }

    public FRangeDerivedColumnAppender(int clusterNumber, int iterNumber,
                                       String rangeDerivedColumnSuffix, List<FColumnValueRange> rangeInfos,
                                       SparkSession spark) {
        this.rangeDerivedColumnSuffix = rangeDerivedColumnSuffix;
        this.rangeInfos = rangeInfos;
        this.spark = spark;
    }
}
