package com.sics.rock.tableinsight4.procedure.preproces;

import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @author zhaorx
 */
public class FDatasetCastHandler {

    private static final Logger logger = LoggerFactory.getLogger(FDatasetCastHandler.class);

    /**
     * Use column names from columnInfos to build cast sql clause
     * So besides cast type, the unspecified columns in the dataset are removed.
     */
    public Dataset<Row> retainColumnAndCastType(
            String tableName, String innerTableName,
            Dataset<Row> dataset, ArrayList<FColumnInfo> columnInfos) {

        String cast = columnInfos.stream().map(columnInfo -> {
            String columnName = columnInfo.getColumnName();
            Class<?> valueType = columnInfo.getValueType();
            return FTypeUtils.castSQLClause(columnName, valueType);
        }).collect(Collectors.joining(", "));

        logger.info("Table {} do cast {}", tableName, cast);

        dataset.createOrReplaceTempView(innerTableName);

        final Dataset<Row> castTable = dataset.sqlContext().sql("SELECT " + cast + " FROM " + innerTableName);

        dataset.sqlContext().dropTempTable(innerTableName);

        return castTable;
    }

}
