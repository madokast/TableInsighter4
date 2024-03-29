package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstantInfo;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FExternalConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FIConstantSearcher;
import com.sics.rock.tableinsight4.preprocessing.constant.search.FRatioConstantSearcher;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * constant handler
 *
 * @author zhaorx
 */
public class FConstantHandler implements FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FConstantHandler.class);

    public void generateConstant(FTableDatasetMap tableDatasetMap) {
        // set for distinct
        final Set<FConstantInfo> allConstantInfo = new HashSet<>();

        // External
        {
            final FIConstantSearcher constantSearcher = new FExternalConstantSearcher();
            final List<FConstantInfo> constantInfos = constantSearcher.search(tableDatasetMap);
            allConstantInfo.addAll(constantInfos);
        }

        // Ratio
        {
            final FIConstantSearcher constantSearcher = new FRatioConstantSearcher(
                    config().findNullConstant, config().constantUpperLimitRatio, config().constantDownLimitRatio
            );
            final List<FConstantInfo> constantInfos = constantSearcher.search(tableDatasetMap);
            allConstantInfo.addAll(constantInfos);
        }

        printConstants(allConstantInfo);
        addToColumnInfo(allConstantInfo, tableDatasetMap);
    }

    private void printConstants(Set<FConstantInfo> constantInfos) {
        for (FConstantInfo constantInfo : constantInfos) {
            logger.info("Find constants {}", constantInfo);
        }
    }

    // tabName -> colName -> colInfo
    private final Map<String, Map<String, FColumnInfo>> columnMap = new HashMap<>();

    private void addToColumnInfo(Set<FConstantInfo> constantInfos, FTableDatasetMap tableDatasetMap) {

        for (FConstantInfo constantInfo : constantInfos) {
            final String tableName = constantInfo.getTableName();
            final String columnName = constantInfo.getColumnName();
            final Map<String, FColumnInfo> columnInfoMap = columnMap.computeIfAbsent(tableName, tn -> tableDatasetMap.getTableInfoByTableName(tn).columnMapView());
            final FColumnInfo columnInfo = columnInfoMap.get(columnName);
            if (columnInfo == null) {
                logger.warn("The column {}.{} does not exist when adding constant info {}", tableName, columnName, constantInfo);
                break;
            }
            columnInfo.addConstant(constantInfo.getConstant());
        }

        // sort constants for consistency
        for (final FTableInfo tableInfo : tableDatasetMap.allTableInfos()) {
            for (final FColumnInfo column : tableInfo.getColumns()) {
                final ArrayList<FConstant<?>> constants = column.getConstants();
                constants.sort(Comparator.comparing(FConstant::toUserString));
            }
        }
    }
}
