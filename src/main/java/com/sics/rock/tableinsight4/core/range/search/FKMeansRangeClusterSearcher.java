package com.sics.rock.tableinsight4.core.range.search;

import com.sics.rock.tableinsight4.core.range.FColumnValueRange;
import com.sics.rock.tableinsight4.core.range.FRange;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FValueRangeConfig;
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
import java.util.stream.Stream;

public class FKMeansRangeClusterSearcher implements FIRangeClusterSearcher {

    private static final Logger logger = LoggerFactory.getLogger(FKMeansRangeClusterSearcher.class);

    private final int clusterNumber;

    private final int iterNumber;

    private final boolean leftEq;

    private final boolean rightEq;

    @Override
    public List<FColumnValueRange> search(FTableDatasetMap tableDatasetMap) {
        List<FColumnValueRange> ret = new ArrayList<>();
        tableDatasetMap.foreach((tab, data) -> {
            final Set<FColumnValueRange> ranges = searchTableRange(tab, data);
            ret.addAll(ranges);
        });
        return ret;
    }

    private Set<FColumnValueRange> searchTableRange(FTableInfo tableInfo, Dataset<Row> dataset) {
        final String tableName = tableInfo.getTableName();
        return tableInfo.rangeColumns().parallelStream()
                .map(columnInfo -> searchColumnRange(dataset, tableName, columnInfo))
                .filter(Optional::isPresent).map(Optional::get)
                .collect(Collectors.toConcurrentMap(Function.identity(), ignore -> 1)).keySet();

    }

    private Optional<FColumnValueRange> searchColumnRange(Dataset<Row> dataset, String tableName, FColumnInfo columnInfo) {
        final FValueRangeConfig rangeConstantInfo = columnInfo.getRangeConstantInfo();
        final String columnName = columnInfo.getColumnName();

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
        final List<FRange> ranges = FKMeansUtils.findRanges(doubleRDD, clusterNumber, iterNumber).stream()
                .map(lr -> new FRange(lr._k, lr._v, leftEq, rightEq)).collect(Collectors.toList());

        return Optional.of(new FColumnValueRange(tableName, columnName, ranges));
    }

    public FKMeansRangeClusterSearcher(int clusterNumber, int iterNumber, boolean leftEq, boolean rightEq) {
        this.clusterNumber = clusterNumber;
        this.iterNumber = iterNumber;
        this.leftEq = leftEq;
        this.rightEq = rightEq;
    }
}
