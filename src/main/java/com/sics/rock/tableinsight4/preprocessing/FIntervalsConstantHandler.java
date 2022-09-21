package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FExternalIntervalConstantImporter;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FIIntervalConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.interval.search.FKMeansIntervalConstantSearcher;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        // external
        {
            final FIIntervalConstantSearcher externalIntervalConstantImporter = new FExternalIntervalConstantImporter();
            final List<FIntervalConstantInfo> externalIntervalConstants = externalIntervalConstantImporter.search(tableDatasetMap);
            printIntervalConstants(externalIntervalConstants);
            addToColumnInfo(externalIntervalConstants, tableDatasetMap);
        }

        // kMeans
        {
            final FIIntervalConstantSearcher kMeansIntervalConstantSearcher = new FKMeansIntervalConstantSearcher(
                    config().kMeansClusterNumber, config().kMeansIterNumber, config().intervalLeftClose, config().intervalRightClose
            );
            final List<FIntervalConstantInfo> kMeansIntervalConstants = kMeansIntervalConstantSearcher.search(tableDatasetMap);
            printIntervalConstants(kMeansIntervalConstants);
            addToColumnInfo(kMeansIntervalConstants, tableDatasetMap);
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
