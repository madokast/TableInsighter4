package com.sics.rock.tableinsight4.others;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FSparkSqlUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class FSparkJoinTest extends FSparkEnv {

    @Test
    public void test() {
        Dataset<Row> ages = FSparkSqlUtils.createTable(spark, 2,
                "name1", "age",
                "Zhao RX", 27,
                "Zhang San", 30,
                "Li Si", 20
        );

        Dataset<Row> heights = FSparkSqlUtils.createTable(spark, 2,
                "name2", "height",
                "Zhao RX", 1.83,
                "Zhang San", 1.80,
                "Li Si", 1.70
        );

        Column name1 = ages.col("name1");
        Column name2 = heights.col("name2");

        //+---------+---+---------+------+
        //|    name1|age|    name2|height|
        //+---------+---+---------+------+
        //|  Zhao RX| 27|  Zhao RX|  1.83|
        //|Zhang San| 30|Zhang San|   1.8|
        //|    Li Si| 20|    Li Si|   1.7|
        //+---------+---+---------+------+
        ages.join(heights, name1.equalTo(name2), "left").show();

    }

    @Test
    public void test2() {
        Dataset<Row> ages = FSparkSqlUtils.createTable(spark, 3,
                "id1", "name1", "age",
                2, "Zhao RX", 27,
                2, "Zhang San", 30,
                3, "Zhang San", 20
        );

        Dataset<Row> heights = FSparkSqlUtils.createTable(spark, 3,
                "id2", "name2", "height",
                2, "Zhao RX", 1.83,
                2, "Zhang San", 1.80,
                3, "Zhang San", 1.70
        );

        Column name1 = ages.col("name1");
        Column name2 = heights.col("name2");

        Column id1 = ages.col("id1");
        Column id2 = heights.col("id2");

        //+---+---------+---+---+---------+------+
        //|id1|    name1|age|id2|    name2|height|
        //+---+---------+---+---+---------+------+
        //|  2|  Zhao RX| 27|  2|  Zhao RX|  1.83|
        //|  2|Zhang San| 30|  2|Zhang San|   1.8|
        //|  3|Zhang San| 20|  3|Zhang San|   1.7|
        //+---+---------+---+---+---------+------+
        ages.join(heights, name1.equalTo(name2).and(id1.equalTo(id2)), "left").show();

    }

    @Test
    public void test_drop_foreign_key() {
        Dataset<Row> ages = FSparkSqlUtils.createTable(spark, 2,
                "name1", "age",
                "Zhao RX", 27,
                "Zhang San", 30,
                "Li Si", 20
        );

        Dataset<Row> heights = FSparkSqlUtils.createTable(spark, 2,
                "name2", "height",
                "Zhao RX", 1.83,
                "Zhang San", 1.80,
                "Li Si", 1.70
        );

        Column name1 = ages.col("name1");
        Column name2 = heights.col("name2");

        //+---------+---+------+
        //|    name1|age|height|
        //+---------+---+------+
        //|  Zhao RX| 27|  1.83|
        //|Zhang San| 30|   1.8|
        //|    Li Si| 20|   1.7|
        //+---------+---+------+
        ages.join(heights, name1.equalTo(name2), "left").drop(name2).show();

    }

    @Test
    public void test_drop_foreign_key_but_name_equal() {
        Dataset<Row> ages = FSparkSqlUtils.createTable(spark, 2,
                "name", "age",
                "Zhao RX", 27,
                "Zhang San", 30,
                "Li Si", 20
        );

        Dataset<Row> heights = FSparkSqlUtils.createTable(spark, 2,
                "name", "height",
                "Zhao RX", 1.83,
                "Zhang San", 1.80,
                "Li Si", 1.70
        );

        Column name1 = ages.col("name");
        Column name2 = heights.col("name");

        //+---------+---+------+
        //|     name|age|height|
        //+---------+---+------+
        //|  Zhao RX| 27|  1.83|
        //|Zhang San| 30|   1.8|
        //|    Li Si| 20|   1.7|
        //+---------+---+------+
        ages.join(heights, name1.equalTo(name2), "left").drop(name2).show();

    }
}
