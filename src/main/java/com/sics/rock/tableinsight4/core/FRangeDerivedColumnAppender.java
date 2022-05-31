package com.sics.rock.tableinsight4.core;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaorx
 */
public class FRangeDerivedColumnAppender {

    private static final Logger logger = LoggerFactory.getLogger(FRangeDerivedColumnAppender.class);

    private final String rangeDerivedColumnSuffix;

    private final SparkSession spark;




    public FRangeDerivedColumnAppender(String rangeDerivedColumnSuffix, SparkSession spark) {
        this.rangeDerivedColumnSuffix = rangeDerivedColumnSuffix;
        this.spark = spark;
    }
}
