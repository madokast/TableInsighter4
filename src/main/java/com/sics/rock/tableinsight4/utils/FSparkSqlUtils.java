package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @author zhaorx
 */
public class FSparkSqlUtils {

    private static final Logger logger = LoggerFactory.getLogger(FSparkSqlUtils.class);

    /**
     * Add an unique id column to the table.
     * Note that it does not start from 0.
     */
    public static Dataset<Row> addRowIdIfAbsent(Dataset<Row> table, String idColumn) {

        for (String column : table.columns()) {
            if (idColumn.equals(column)) return table;
        }

        return table.withColumn(idColumn, functions.monotonically_increasing_id());
    }

    public static Dataset<Row> retainColumnAndCastType(
            String tableName, String innerTableName,
            Dataset<Row> dataset, ArrayList<FColumnInfo> columns) {
        String cast = columns.stream().map(columnInfo -> {
            String columnName = columnInfo.getColumnName();
            Class<?> valueType = columnInfo.getValueType();
            return FTypeUtils.cast(columnName, valueType);
        }).collect(Collectors.joining(", "));

        logger.info("Table {} do cast {}", tableName, cast);

        dataset.createOrReplaceTempView(innerTableName);

        return dataset.sqlContext().sql("SELECT " + cast + " FROM " + innerTableName);
    }


}
