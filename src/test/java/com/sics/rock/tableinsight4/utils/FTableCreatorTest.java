package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.test.FSparkEnv;
import com.sics.rock.tableinsight4.test.FTableCreator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class FTableCreatorTest extends FSparkEnv {

    @Test
    public void test() {
        final Dataset<Row> table = FTableCreator.use(spark).create("name,age", "a,30", "b, 40");
        table.show();
    }

}
