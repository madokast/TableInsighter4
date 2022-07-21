package com.sics.rock.tableinsight4.procedure;

import com.sics.rock.tableinsight4.procedure.constant.FConstantInfo;
import com.sics.rock.tableinsight4.procedure.constant.search.FExternalConstantSearcher;
import com.sics.rock.tableinsight4.procedure.constant.search.FIConstantSearcher;
import com.sics.rock.tableinsight4.procedure.constant.search.FRatioConstantSearcher;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
    }
}
