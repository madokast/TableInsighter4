package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.procedure.FPliConstructor;
import com.sics.rock.tableinsight4.procedure.FTableDataLoader;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;

public class FPliConstructorTest extends FTableInsightEnv {

    @Test
    public void construct() {

        FTableInfo relation = FExamples.relation();

        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        FPliConstructor constructor = new FPliConstructor(
                config().idColumnName,
                2,
                spark
        );

        FPLI PLI = constructor.construct(tableDatasetMap);

    }

    @Test
    public void construct2() {

        FTableInfo relation = FExamples.relation();

        FTableDatasetMap tableDatasetMap = new FTableDataLoader().prepareData(Collections.singletonList(relation));

        FPliConstructor constructor = new FPliConstructor(
                config().idColumnName,
                1000,
                spark
        );

        FPLI PLI = constructor.construct(tableDatasetMap);

    }
}