package com.sics.rock.tableinsight4.preprocessing.interval.search;

import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FKMeansUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FKMeansIntervalConstantSearcher implements FIIntervalConstantSearcher {

    private static final Logger logger = LoggerFactory.getLogger(FKMeansIntervalConstantSearcher.class);

    private final int clusterNumber;

    private final int iterNumber;

    private final boolean leftClose;

    private final boolean rightClose;

    @Override
    public List<FIntervalConstantInfo> search(FTableDatasetMap tableDatasetMap) {
        List<FIntervalConstantInfo> ret = new ArrayList<>();
        tableDatasetMap.foreach((tab, data) -> {
            final Set<FIntervalConstantInfo> intervals = searchTableIntervals(tab, data);
            ret.addAll(intervals);
        });
        return ret;
    }

    private Set<FIntervalConstantInfo> searchTableIntervals(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        return tableInfo.intervalRequiredColumnsView().parallelStream()
                .map(columnInfo -> searchColumnIntervals(dataset, tableName, columnInfo))
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toConcurrentMap(Function.identity(), ignore -> 1)).keySet();

    }

    private Optional<FIntervalConstantInfo> searchColumnIntervals(Dataset<Row> dataset, String tableName, FColumnInfo columnInfo) {
        final FIntervalConstantConfig intervalConstantInfo = columnInfo.getIntervalConstantInfo();
        final String columnName = columnInfo.getColumnName();
        final FValueType valueType = columnInfo.getValueType();

        final JavaRDD<Double> doubleRDD = dataset.selectExpr(FTypeUtils.castSQLClause(columnName, FValueType.DOUBLE))
                .toJavaRDD().map(r -> (Double) r.get(0))
                .filter(Objects::nonNull)
                .filter(num -> !Double.isNaN(num))
                .filter(num -> !Double.isInfinite(num))
                .cache().setName("Interval_" + tableName + "_" + columnName);
        final long distinctCount = doubleRDD.distinct().count();
        if (distinctCount < 3) {
            logger.info("The nonnull distinct count of {}.{} is {}. Skip generating intervals constants", tableName,
                    columnName, distinctCount);
            return Optional.empty();
        }
        final int iterNumber = intervalConstantInfo.getConfig(FIntervalConstantConfig.CONFIG_K_MEANS_ITER_NUMBER, this.iterNumber);
        final int clusterNumber = intervalConstantInfo.getConfig(FIntervalConstantConfig.CONFIG_K_MEANS_CLUSTER_NUMBER, this.clusterNumber);
        final boolean leftEq = intervalConstantInfo.getConfig(FIntervalConstantConfig.CONFIG_LEFT_CLOSE, this.leftClose);
        final boolean rightEq = intervalConstantInfo.getConfig(FIntervalConstantConfig.CONFIG_RIGHT_CLOSE, this.rightClose);
        final List<FInterval<?>> intervals = FKMeansUtils.findIntervals(doubleRDD, clusterNumber, iterNumber).stream()
                .map(lr -> new FInterval<>(lr._k, lr._v, leftEq, rightEq).typeCast(valueType))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (intervals.isEmpty()) {
            logger.info("Cannot find intervals constants in {}.{}", tableName, columnName);
            return Optional.empty();
        }

        return Optional.of(new FIntervalConstantInfo(tableName, columnName, intervals, FIntervalConstantInfo.SOURCE_K_MEANS));
    }

    public FKMeansIntervalConstantSearcher(int clusterNumber, int iterNumber, boolean leftClose, boolean rightClose) {
        this.clusterNumber = clusterNumber;
        this.iterNumber = iterNumber;
        this.leftClose = leftClose;
        this.rightClose = rightClose;
    }
}
