package com.sics.rock.tableinsight4.others;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FSparkSqlUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.apache.spark.sql.functions.*;

public class FSparkGroupByTest extends FSparkEnv {

    @Test
    public void test() {
        Dataset<Row> exam = FSparkSqlUtils.createTable(spark, 2,
                "name", "score",
                "Zhao RX", 85,
                "Zhao RX", 86,
                "Li Si", 90,
                "Li Si", 91
        );

        Column name = exam.col("name");
        Column score = exam.col("score");

        //tab
        //+-------+---------+---------+---------+---------+-----------+
        //|   name|max_score|min_score|sum_score|avg_score|count_score|
        //+-------+---------+---------+---------+---------+-----------+
        //|Zhao RX|       86|       85|      171|     85.5|          2|
        //|  Li Si|       91|       90|      181|     90.5|          2|
        //+-------+---------+---------+---------+---------+-----------+
        exam.groupBy(name).agg(
                max(score).as("max_score"),
                min(score).as("min_score"),
                sum(score).as("sum_score"),
                avg(score).as("avg_score"),
                count(score).as("count_score")
        ).show();
    }
}
