package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.test.FSparkTestEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Collections;

public class FSparkSqlUtilsTest extends FSparkTestEnv {


    @Test
    public void test_createTable() {
        final Dataset<Row> table = FSparkSqlUtils.createTable(spark, 3,
                "name", "age", "height",
                "Zhao RX", 27, 1.83,
                "Zhang San", 37, 1.84,
                "Li Si", null, 1.85,
                "Wang Wu", 47, null
        );

        table.show();
    }

    @Test
    public void test_leftOuterJoin() {
        final Dataset<Row> table1 = FSparkSqlUtils.createTable(spark, 2,
                "name", "age",
                "Zhao RX", 27,
                "Zhang San", 37,
                "Li Si", null,
                "Wang Wu", 47
        );

        final Dataset<Row> table2 = FSparkSqlUtils.createTable(spark, 2,
                "name", "height",
                "Zhao RX", 1.83,
                "Zhang San", 1.84,
                "Li Si", 1.85,
                "Wang Wu", null
        );

        final Dataset<Row> join = FSparkSqlUtils.leftOuterJoin(table1, table2, Collections.singletonList("name"));

        join.show();
    }

    @Test
    public void test_leftOuterJoin2() {
        final Dataset<Row> table1 = FSparkSqlUtils.createTable(spark, 3,
                "name", "age", "key",
                "Zhao RX", 27, 1,
                "Zhang San", 37, 2,
                "Li Si", null, 3,
                "Wang Wu", 47, 4
        );

        final Dataset<Row> table2 = FSparkSqlUtils.createTable(spark, 3,
                "name", "height", "key",
                "Zhao RX", 1.83, 1,
                "Zhang San", 1.84, 2,
                "Li Si", 1.85, 3,
                "Wang Wu", null, 4
        );

        final Dataset<Row> join = FSparkSqlUtils.leftOuterJoin(table1, table2, FUtils.listOf("name", "key"));

        join.show();
    }

    @Test
    public void test_leftOuterJoin3() {
        final Dataset<Row> table1 = FSparkSqlUtils.createTable(spark, 3,
                "name", "age", "key",
                "Zhang San", 37, 2,
                "Zhao RX", 27, 1,
                "Wang Wu", 47, 4,
                "Li Si", null, 3
        );

        final Dataset<Row> table2 = FSparkSqlUtils.createTable(spark, 3,
                "name", "height", "key",
                "Li Si", 1.85, 3,
                "Wang Wu", null, 4,
                "Zhang San", 1.84, 2,
                "Zhao RX", 1.83, 1
        );

        final Dataset<Row> join = FSparkSqlUtils.leftOuterJoin(table1, table2, FUtils.listOf("name", "key"));

        join.show();
    }

    @Test
    public void test_leftOuterJoin4() {
        final Dataset<Row> table1 = FSparkSqlUtils.createTable(spark, 3,
                "name", "age", "key",
                "Zhang San", 37, 2,
                "Zhao RX", 27, 1,
                "Wang Wu", 47, 4,
                "Li Si", null, 3
        );

        final Dataset<Row> table2 = FSparkSqlUtils.createTable(spark, 3,
                "name", "height", "key",
                "Li Si", 1.85, 3,
                "Wang Wu", null, 4
        );

        final Dataset<Row> join = FSparkSqlUtils.leftOuterJoin(table1, table2, FUtils.listOf("name", "key"));

        join.show();
    }
}