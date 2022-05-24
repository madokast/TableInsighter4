package com.sics.rock.tableinsight4.utils;


import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.FSparkEnv;
import com.sics.rock.tableinsight4.test.FTableData;
import org.junit.Test;

public class FExamplesTest extends FSparkEnv {

    @Test
    public void test_relation() {
        FTableData relation = FExamples.use(spark).relation();
        relation.tableData.show();
    }


}
