package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;

public class FTableDataLoaderTest extends FTableInsightEnv {

    @Test
    public void prepareData() {

        FTableDataLoader preprocess = new FTableDataLoader();

        FTableInfo relation = FExamples.relation();

        final FTableDatasetMap tableDatasetMap = preprocess.prepareData(Collections.singletonList(relation));

        tableDatasetMap.getDatasetByInnerTableName(relation.getInnerTableName()).show();
    }

    @Test
    public void prepareData2() {

        FTableDataLoader preprocess = new FTableDataLoader();

        FTableInfo doubleNumberNullColumn7 = FExamples.doubleNumberNullColumn7();

        final FTableDatasetMap tableDatasetMap = preprocess.prepareData(Collections.singletonList(doubleNumberNullColumn7));

        tableDatasetMap.getDatasetByTableName(doubleNumberNullColumn7.getTableName()).show();
    }
}