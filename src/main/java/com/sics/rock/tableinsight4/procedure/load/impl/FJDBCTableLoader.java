package com.sics.rock.tableinsight4.procedure.load.impl;

import com.sics.rock.tableinsight4.procedure.load.FITableLoader;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Properties;

/**
 * JDBC table loader
 *
 * @author zhaorx
 */
public class FJDBCTableLoader implements FITableLoader {

    private final SparkSession spark;

    private final String url;

    private final Properties properties = new Properties();

    public FJDBCTableLoader(SparkSession spark, Map<String, String> options) {
        FAssertUtils.require(options.containsKey("url"), "url is required to load data from JDBC");
        FAssertUtils.require(options.containsKey("user"), "user is required to load data from JDBC");
        FAssertUtils.require(options.containsKey("password"), "password is required to load data from JDBC");

        this.spark = spark;
        this.url = options.get("url");

        options.forEach(properties::setProperty);
    }


    @Override
    public Dataset<Row> load(String tablePath) {
        return spark.read().jdbc(url, tablePath, properties);
    }

    public static boolean canLoad(String tablePath, Map<String, String> options) {
        if (options == null || options.isEmpty()) return false;
        else return options.getOrDefault("url", "").length() > 5;
    }
}
