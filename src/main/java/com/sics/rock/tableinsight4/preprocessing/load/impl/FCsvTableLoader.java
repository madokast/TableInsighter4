package com.sics.rock.tableinsight4.preprocessing.load.impl;

import com.sics.rock.tableinsight4.preprocessing.load.FITableLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * csv reader
 * options see https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html
 *
 * @author zhaorx
 */
public class FCsvTableLoader implements FITableLoader {

    private final SparkSession spark;

    /**
     * ti.data.load.csv.options
     */
    private final Map<String, String> options;

    public FCsvTableLoader(SparkSession spark, Map<String, String> options) {
        this.spark = spark;
        this.options = options;
    }

    @Override
    public Dataset<Row> load(String tablePath) {
        return spark.read().options(options).csv(tablePath);
    }

    public static boolean canLoad(String tablePath, Map<String, String> options) {
        return tablePath.toLowerCase().trim().endsWith("csv");
    }
}
