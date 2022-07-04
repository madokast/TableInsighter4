package com.sics.rock.tableinsight4.procedure.interval.search;

import com.sics.rock.tableinsight4.procedure.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.junit.Test;

import java.util.List;

public class FExternalRangeConstantImporterTest extends FSparkEnv {

    @Test
    public void search() {
        final FTableInfo relation = FExamples.relation();
        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(relation, spark.emptyDataFrame());

        final FIntervalConstantConfig r0 = FIntervalConstantConfig.findIntervalConstant();
        final FIntervalConstantConfig r1 = FIntervalConstantConfig.findIntervalConstant();
        relation.getColumns().get(0).setIntervalConstantInfo(r0);
        relation.getColumns().get(1).setIntervalConstantInfo(r1);

        r0.addExternalIntervalConstant("5");
        r1.addExternalIntervalConstant("<10");

        final FExternalIntervalConstantImporter importer = new FExternalIntervalConstantImporter();
        final List<FIntervalConstantInfo> results = importer.search(tableDatasetMap);
        for (FIntervalConstantInfo result : results) {
            logger.info("find {}", result);
        }
    }


    @Test
    public void search2() {
        final FTableInfo relation = FExamples.relation();
        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(relation, spark.emptyDataFrame());

        final FIntervalConstantConfig r0 = FIntervalConstantConfig.findIntervalConstant();
        final FIntervalConstantConfig r1 = FIntervalConstantConfig.findIntervalConstant();
        relation.getColumns().get(2).setIntervalConstantInfo(r0);
        relation.getColumns().get(3).setIntervalConstantInfo(r1);

        r0.addExternalIntervalConstant(">=53.3");
        r1.addExternalIntervalConstant("[3, 4)");

        final FExternalIntervalConstantImporter importer = new FExternalIntervalConstantImporter();
        final List<FIntervalConstantInfo> results = importer.search(tableDatasetMap);
        for (FIntervalConstantInfo result : results) {
            logger.info("find {}", result);
        }
    }
}