package com.sics.rock.tableinsight4.processor;

import com.sics.rock.tableinsight4.table.FTable;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.FSparkEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FTableDataLoaderTest extends FSparkEnv {

    @Test
    public void prepareData() {

        FTableDataLoader preprocess = new FTableDataLoader(spark, "row_id");

        FTableInfo relation = FExamples.relation();

        List<FTable> tabs = preprocess.prepareData(Collections.singletonList(relation));

        FTable tab = tabs.get(0);

        Dataset<Row> dataset = tab.getDataset();

        dataset.show();
    }

    @Test
    public void prepareData2() {

        FTableDataLoader preprocess = new FTableDataLoader(spark, "row_id");

        FTableInfo doubleNumberNullColumn7 = FExamples.doubleNumberNullColumn7();

        List<FTable> tabs = preprocess.prepareData(Collections.singletonList(doubleNumberNullColumn7));

        FTable tab = tabs.get(0);

        Dataset<Row> dataset = tab.getDataset();

        dataset.show();
    }
}