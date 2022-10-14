package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FConstantInfoIntervalConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FExternalIntervalConstantImporter;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FIIntervalConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FKMeansIntervalConstantSearcher;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * interval constant handler
 *
 * @author zhaorx
 */
public class FIntervalsConstantHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FIntervalsConstantHandler.class);

    public void generateIntervalConstant(FTableDatasetMap tableDatasetMap) {
        List<FIIntervalConstantSearcher> intervalConstantSearchers = new ArrayList<>();

        // external interval info
        intervalConstantSearchers.add(new FExternalIntervalConstantImporter());

        // kMeans algorithm finding intervals
        if (config().kMeansIntervalSearch) intervalConstantSearchers.add(new FKMeansIntervalConstantSearcher(
                config().kMeansClusterNumber, config().kMeansIterNumber,
                config().intervalLeftClose, config().intervalRightClose, config().kMeansRandomSeed));

        // create intervals form constant
        if (config().usingConstantCreateInterval) intervalConstantSearchers.add(
                new FConstantInfoIntervalConstantSearcher(config().constantIntervalOperators));

        for (final FIIntervalConstantSearcher intervalConstantSearcher : intervalConstantSearchers) {
            final List<FIntervalConstantInfo> intervalConstantInfos = intervalConstantSearcher.search(tableDatasetMap);
            printIntervalConstants(intervalConstantInfos);
            addToColumnInfo(intervalConstantInfos, tableDatasetMap);
        }
    }

    private void printIntervalConstants(List<FIntervalConstantInfo> intervalConstants) {
        for (FIntervalConstantInfo intervalConstant : intervalConstants) {
            logger.info("Find interval constants {}", intervalConstant);
        }
    }

    // tabName -> colName -> colInfo
    private final Map<String, Map<String, FColumnInfo>> columnMap = new HashMap<>();

    private void addToColumnInfo(List<FIntervalConstantInfo> intervalConstants, FTableDatasetMap tableDatasetMap) {

        for (FIntervalConstantInfo intervalConstant : intervalConstants) {
            final String tableName = intervalConstant.getTableName();
            final String columnName = intervalConstant.getColumnName();
            final Map<String, FColumnInfo> columnInfoMap = columnMap.computeIfAbsent(tableName, tn -> tableDatasetMap.getTableInfoByTableName(tn).columnMapView());
            final FColumnInfo columnInfo = columnInfoMap.get(columnName);
            if (columnInfo == null) {
                logger.warn("The column {}.{} does not exist when adding interval-constant info {}", tableName, columnName, intervalConstant);
                break;
            }
            columnInfo.addIntervalConstants(intervalConstant.getIntervals());
        }
    }

}
