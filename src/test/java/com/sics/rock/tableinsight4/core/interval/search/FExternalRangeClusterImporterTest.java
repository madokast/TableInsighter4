package com.sics.rock.tableinsight4.core.interval.search;

import com.sics.rock.tableinsight4.core.interval.FIntervalConstant;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.junit.Test;

import java.util.List;

public class FExternalRangeClusterImporterTest extends FSparkEnv {

    @Test
    public void search() {
        final FTableInfo relation = FExamples.relation();
        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(relation, spark.emptyDataFrame());

        final FIntervalConstantConfig r0 = FIntervalConstantConfig.findUsingDefaultConfig();
        final FIntervalConstantConfig r1 = FIntervalConstantConfig.findUsingDefaultConfig();
        relation.getColumns().get(0).setIntervalConstantInfo(r0);
        relation.getColumns().get(1).setIntervalConstantInfo(r1);

        r0.addExternalIntervalConstant("5");
        r1.addExternalIntervalConstant("<10");

        final FExternalIntervalClusterImporter importer = new FExternalIntervalClusterImporter();
        final List<FIntervalConstant> results = importer.search(tableDatasetMap);
        for (FIntervalConstant result : results) {
            logger.info("find {}", result);
        }
    }


    @Test
    public void search2() {
        final FTableInfo relation = FExamples.relation();
        final FTableDatasetMap tableDatasetMap = new FTableDatasetMap();
        tableDatasetMap.put(relation, spark.emptyDataFrame());

        final FIntervalConstantConfig r0 = FIntervalConstantConfig.findUsingDefaultConfig();
        final FIntervalConstantConfig r1 = FIntervalConstantConfig.findUsingDefaultConfig();
        relation.getColumns().get(2).setIntervalConstantInfo(r0);
        relation.getColumns().get(3).setIntervalConstantInfo(r1);

        r0.addExternalIntervalConstant(">=53.3");
        r1.addExternalIntervalConstant("[3, 4)");

        final FExternalIntervalClusterImporter importer = new FExternalIntervalClusterImporter();
        final List<FIntervalConstant> results = importer.search(tableDatasetMap);
        for (FIntervalConstant result : results) {
            logger.info("find {}", result);
        }
    }
}