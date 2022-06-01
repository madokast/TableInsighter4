package com.sics.rock.tableinsight4.core;

import com.sics.rock.tableinsight4.core.range.FColumnValueRange;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumns;
import com.sics.rock.tableinsight4.table.column.FValueRangeConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FKMeansUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author zhaorx
 */
public class FRangeDerivedColumnAppender {

    private static final Logger logger = LoggerFactory.getLogger(FRangeDerivedColumnAppender.class);

    private final String rangeDerivedColumnSuffix;

    // Key is tabName.colName (FColumns::columnIdentifier)
    // TODO FRange
    private final Map<String, FColumnValueRange> rangeInfoMap = new ConcurrentHashMap<>();

    private final int clusterNumber;

    private final int iterNumber;

    private final SparkSession spark;

    public Stream<FColumnValueRange> searchColumnRange(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        return tableInfo.rangeColumns().parallelStream()
                .map(columnInfo -> searchColumnRangeConstant(dataset, tableName, columnInfo))
                .filter(Optional::isPresent).map(Optional::get);
    }

    private Optional<FColumnValueRange> searchColumnRangeConstant(Dataset<Row> dataset, String tableName, FColumnInfo columnInfo) {
        final FValueRangeConfig rangeConstantInfo = columnInfo.getRangeConstantInfo();
        final String columnName = columnInfo.getColumnName();
        if (rangeInfoMap.containsKey(FColumns.columnIdentifier(tableName, columnName))) {
            logger.info("The range constant info of {}.{} has been specified externally", tableName, columnInfo);
            return Optional.empty();
        }

        final JavaRDD<Double> doubleRDD = dataset.selectExpr(FTypeUtils.castSQLClause(columnName, FValueType.DOUBLE))
                .toJavaRDD().map(r -> r.getDouble(0)).filter(Objects::nonNull)
                .cache().setName("RANGE_" + tableName + "_" + columnName);
        final long distinctCount = doubleRDD.distinct().count();
        if (distinctCount < 3) {
            logger.info("The distinct count of  {}.{} is {}. Skip generating range constants", tableName,
                    columnName, distinctCount);
            return Optional.empty();
        }
        final int iterNumber = rangeConstantInfo.getIterNumber(this.iterNumber);
        final int clusterNumber = rangeConstantInfo.getClusterNumber(this.clusterNumber);
        final List<FPair<Double, Double>> ranges = FKMeansUtils.findRanges(doubleRDD, clusterNumber, iterNumber);

        return Optional.of(new FColumnValueRange(tableName, columnName, ranges));
    }

    public void appendRangeDerivedColumn(FTableDatasetMap tableDatasetMap) {
        rangeInfoMap.values().stream().sequential().forEach(columnRange->{
            final String tableName = columnRange.getTableName();
            final String columnName = columnRange.getColumnName();
            final List<FPair<Double, Double>> ranges = columnRange.getRanges();

            final String derivedColumnName = rangeDerivedColumnSuffix + columnName;
            final Dataset<Row> tableData = tableDatasetMap.getDatasetByTableName(tableName);
            final Column col = tableData.col(columnName);

        });
    }

    public FRangeDerivedColumnAppender(int clusterNumber, int iterNumber,
                                       String rangeDerivedColumnSuffix, List<FColumnValueRange> externalRangeInfos,
                                       SparkSession spark) {
        this.clusterNumber = clusterNumber;
        this.iterNumber = iterNumber;
        this.rangeDerivedColumnSuffix = rangeDerivedColumnSuffix;
        externalRangeInfos.forEach(info -> rangeInfoMap.put(info.identifier(), info));
        this.spark = spark;
    }
}
