package com.sics.rock.tableinsight4.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FTableCreator {

    private static final String NEW_LINE = System.lineSeparator();

    public Dataset<Row> create(String header, String... lines) {
        FTempFile tab = FTempFile.create(header + NEW_LINE + String.join(NEW_LINE, lines));
        return spark.read()
                .option("header", "true")
                .csv(tab.getPath().getAbsolutePath());

    }

    private SparkSession spark;

    public static FTableCreator use(SparkSession spark) {
        FTableCreator t = new FTableCreator();
        t.spark = spark;
        return t;
    }

}
