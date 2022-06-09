package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.interval.FIntervalConstant;
import com.sics.rock.tableinsight4.core.interval.search.FExternalIntervalClusterImporter;
import com.sics.rock.tableinsight4.core.interval.search.FIIntervalClusterSearcher;
import com.sics.rock.tableinsight4.core.interval.search.FKMeansIntervalClusterSearcher;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhaorx
 */
public class FIntervalsConstantHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FIntervalsConstantHandler.class);

    public void generateIntervalConstant(FTableDatasetMap tableDatasetMap) {
        // externalIntervalClusterImporter
        final FIIntervalClusterSearcher externalIntervalClusterImporter = new FExternalIntervalClusterImporter();
        final List<FIntervalConstant> externalIntervalConstants = externalIntervalClusterImporter.search(tableDatasetMap);
        printIntervalConstants(externalIntervalConstants);
        addToColumnInfo(externalIntervalConstants, tableDatasetMap);

        // kMeansIntervalClusterSearcher
        final FIIntervalClusterSearcher kMeansIntervalClusterSearcher = new FKMeansIntervalClusterSearcher(
                config().kMeansClusterNumber, config().kMeansIterNumber, config().intervalLeftClose, config().intervalRightClose
        );
        final List<FIntervalConstant> kMeansIntervalConstants = kMeansIntervalClusterSearcher.search(tableDatasetMap);
        printIntervalConstants(kMeansIntervalConstants);
        addToColumnInfo(kMeansIntervalConstants, tableDatasetMap);
    }

    private void printIntervalConstants(List<FIntervalConstant> intervalConstants) {
        for (FIntervalConstant intervalConstant : intervalConstants) {
            logger.info("Find interval constants {}", intervalConstant);
        }
    }

    private final Map<String, Map<String, FColumnInfo>> columnMap = new HashMap<>();

    private void addToColumnInfo(List<FIntervalConstant> intervalConstants, FTableDatasetMap tableDatasetMap) {

        for (FIntervalConstant intervalConstant : intervalConstants) {
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
