package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.test.FSparkTestEnv;
import com.sics.rock.tableinsight4.test.FTableCreator;
import org.junit.Test;

import java.io.File;

public class FTableCreatorTest extends FSparkTestEnv {

    @Test
    public void test() {
        final File table = FTableCreator.createCsv("name,age", "a,30", "b, 40");
        spark.read().option("header", "true").csv(table.getAbsolutePath()).show();
    }

}
