package com.sics.rock.tableinsight4.utils;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * spark-sql utils
 *
 * @author zhaorx
 */
public class FSparkSqlUtils {

    private static final Logger logger = LoggerFactory.getLogger(FSparkSqlUtils.class);

    public static Dataset<Row> leftOuterJoin(Dataset<Row> leftTab, Dataset<Row> rightTab, List<String> onColumns) {

        if (onColumns == null || onColumns.isEmpty()) throw new IllegalArgumentException("onColumns is null or empty");

        // spark bug https://issues.apache.org/jira/browse/SPARK-15384
        // final Seq<String> onCols = JavaConversions.asScalaIterator(onColumns.iterator()).toSeq();
        // return leftTab.join(rightTab, onCols, "left_outer");

        final String suffix = "_LJ";

        for (String onColumn : onColumns) {
            rightTab = rightTab.withColumnRenamed(onColumn, onColumn + suffix);
        }

        final Dataset<Row> renamedRightTab = rightTab;

        final Column cond = onColumns.stream().map(onCol ->
                leftTab.col(onCol).equalTo(renamedRightTab.col(onCol + suffix))
        ).reduce(Column::and).orElseThrow(() -> new IllegalArgumentException("Join condition is empty. " + onColumns));
        Dataset<Row> join = leftTab.join(rightTab, cond, "left");

        for (String onColumn : onColumns) {
            join = join.drop(onColumn + suffix);
        }

        return join;
    }

    public static Dataset<Row> createTable(SparkSession spark, int columnSize, Object... columnNameValue) {
        final String[] columns = new String[columnSize];
        final boolean[] nullableArr = new boolean[columnSize];
        final DataType[] types = new DataType[columnSize];
        final List<Object[]> rows = new ArrayList<>();

        Arrays.fill(nullableArr, false);

        for (int i = 0; i < columnSize; i++) {
            columns[i] = (String) columnNameValue[i];
        }
        logger.debug("Create table columns = {}", Arrays.toString(columns));

        for (int i = columnSize; i < columnNameValue.length; i++) {
            final Object val = columnNameValue[i];
            final Object[] row;
            final int colIndex = i % columnSize;
            if (colIndex == 0) {
                // New row
                row = new Object[columnSize];
                rows.add(row);
            } else {
                row = rows.get(rows.size() - 1);
            }

            row[colIndex] = val;
            if (val == null) nullableArr[colIndex] = true;
            else {
                final DataType dataType = FTypeUtils.toSparkDataType(val.getClass());
                if (types[colIndex] == null) types[colIndex] = dataType;
                else if (!types[colIndex].equals(dataType)) {
                    throw new IllegalArgumentException("Date type inconsistent at " + columns[colIndex] + " column. " +
                            "May be " + types[colIndex] + " or " + dataType);
                }
            }
        }

        logger.debug("Create table nullable = {}", Arrays.toString(nullableArr));
        logger.debug("Create table types = {}", Arrays.toString(types));

        final List<Row> rowList = rows.stream().map(RowFactory::create).collect(Collectors.toList());

        StructType schema = new StructType();

        for (int i = 0; i < columnSize; i++) {
            DataType type = types[i];
            if (type == null) type = DataTypes.StringType;
            schema = schema.add(columns[i], type, nullableArr[i]);
        }

        return spark.createDataFrame(rowList, schema);

    }

}
