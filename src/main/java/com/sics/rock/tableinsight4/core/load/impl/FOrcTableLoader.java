package com.sics.rock.tableinsight4.core.load.impl;


import com.sics.rock.tableinsight4.core.load.FITableLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;


/**
 * orc reader
 *
 * @author zhaorx
 */
public class FOrcTableLoader implements FITableLoader {

    private final SparkSession spark;

    private final Map<String, String> options;

    public FOrcTableLoader(SparkSession spark, Map<String, String> options) {
        this.spark = spark;
        this.options = options;
    }

    @Override
    public Dataset<Row> load(String tablePath) {
        return spark.read().options(options).orc(tablePath);
    }

    public static boolean canLoad(String tablePath, Map<String, String> options) {
        // default
        return true;
    }
}
