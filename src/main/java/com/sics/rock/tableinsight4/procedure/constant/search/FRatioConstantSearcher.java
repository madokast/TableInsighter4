package com.sics.rock.tableinsight4.procedure.constant.search;

import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FConstantConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FRatioConstantSearcher implements FIConstantSearcher {

    private static final Logger logger = LoggerFactory.getLogger(FRatioConstantSearcher.class);

    private final boolean findNullConstant;
    private final double upperLimitRatio;
    private final double downLimitRatio;

    @Override
    public List<FConstantInfo> search(FTableDatasetMap tableDatasetMap) {
        final List<FConstantInfo> ret = new ArrayList<>();
        tableDatasetMap.foreach((tabInfo, dataset) -> {
            ret.addAll(search(tabInfo, dataset));
        });

        return ret;
    }

    private List<FConstantInfo> search(FTableInfo tabInfo, Dataset<Row> dataset) {
        final String tableName = tabInfo.getTableName();
        final long length = tabInfo.getLength(dataset::count);
        return tabInfo.getColumns().parallelStream().flatMap(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            final FValueType valueType = columnInfo.getValueType();
            final FConstantConfig constantConfig = columnInfo.getConstantConfig();
            if (columnInfo.isSkip() || !constantConfig.isFindConstant()) return null;

            final boolean findNullConstant = constantConfig.getConfig(FConstantConfig.CONFIG_FIND_NULL_CONSTANT, this.findNullConstant);
            final double upperLimitRatio = constantConfig.getConfig(FConstantConfig.CONFIG_UPPER_LIMIT_RATIO, this.upperLimitRatio);
            final double downLimitRatio = constantConfig.getConfig(FConstantConfig.CONFIG_DOWN_LIMIT_RATIO, this.downLimitRatio);
            final double maxLength = length * upperLimitRatio;
            final double minLength = length * downLimitRatio;

            JavaRDD<?> values = dataset.select(columnName).toJavaRDD().map(row -> row.get(0));
            if (!findNullConstant) values = values.filter(Objects::nonNull);
            return values.mapToPair(v -> new Tuple2<>(v, 1L))
                    .reduceByKey(Long::sum)
                    .filter(t -> t._2 <= maxLength && t._2 >= minLength)
                    .collect()
                    .stream()
                    .peek(t -> logger.debug("Find constant {} occurrence {} in {}.{}", t._1, t._2, tableName, columnName))
                    .map(Tuple2::_1)
                    .peek(cons -> FAssertUtils.require(valueType.instance(cons), cons + " is not instance of " + valueType))
                    .map(FConstant::new)
                    .map(cons -> new FConstantInfo(tableName, columnName, valueType, cons));
        }).collect(Collectors.toList());
    }

    public FRatioConstantSearcher(boolean findNullConstant, double upperLimitRatio, double downLimitRatio) {
        this.findNullConstant = findNullConstant;
        this.upperLimitRatio = upperLimitRatio;
        this.downLimitRatio = downLimitRatio;
    }
}
