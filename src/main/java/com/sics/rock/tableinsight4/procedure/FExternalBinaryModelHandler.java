package com.sics.rock.tableinsight4.procedure;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.procedure.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.procedure.external.FIExternalBinaryModelCalculator;
import com.sics.rock.tableinsight4.table.FColumnInfoFactory;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.utils.FSparkSqlUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FExternalBinaryModelHandler {

    private static final Logger logger = LoggerFactory.getLogger(FExternalBinaryModelHandler.class);

    private final String externalBinaryModelDerivedColumnSuffix;

    private final SparkSession spark;

    private final String idColumn;

    public void appendDerivedColumn(FTableDatasetMap tables, List<FExternalBinaryModelInfo> externalBinaryModelInfos) {
        externalBinaryModelInfos.forEach(ex->appendDerivedColumn(tables, ex));
    }

    // thread unsafe
    private void appendDerivedColumn(FTableDatasetMap tables, FExternalBinaryModelInfo externalPairInfo) {
        // Derived column $EX_id
        final String externalDerivedColumn = externalBinaryModelDerivedColumnSuffix + externalPairInfo.getId();

        final String leftTableName = externalPairInfo.getLeftTableName();
        final String rightTableName = externalPairInfo.getRightTableName();
        final FTableInfo leftTableInfo = tables.getTableInfoByTableName(leftTableName);
        final FTableInfo rightTableInfo = tables.getTableInfoByTableName(rightTableName);
        Dataset<Row> mainLeftTable = tables.getDatasetByTableName(leftTableName);
        Dataset<Row> mainRightTable = tables.getDatasetByTableName(rightTableName);

        // Calculate
        final FIExternalBinaryModelCalculator calculator = externalPairInfo.getCalculator();
        final List<FPair<Long, Long>> pairs = calculator.calculate();
        if (pairs.isEmpty()) return;

        // Build union-find set
        final Dataset<Row> externalTable = buildClassificationTableFromCalculatedModel(externalPairInfo.getCalculator(), externalDerivedColumn);

        // Main left table left join externalTable
        mainLeftTable = FSparkSqlUtils.leftOuterJoin(mainLeftTable, externalTable, Collections.singletonList(idColumn));
        leftTableInfo.addColumnInfo(FColumnInfoFactory.createExternalDerivedColumn(externalDerivedColumn, externalPairInfo));
        tables.updateDataset(leftTableInfo, mainLeftTable);

        // Main right table left join externalTable
        if (!leftTableName.equals(rightTableName)) {
            mainRightTable = FSparkSqlUtils.leftOuterJoin(mainRightTable, externalTable, Collections.singletonList(idColumn));
            rightTableInfo.addColumnInfo(FColumnInfoFactory.createExternalDerivedColumn(externalDerivedColumn, externalPairInfo));
            tables.updateDataset(rightTableInfo, mainRightTable);
        }
    }

    private Dataset<Row> buildClassificationTableFromCalculatedModel(FIExternalBinaryModelCalculator calculator, String derivedColumn) {
        final List<FPair<Long, Long>> pairs = calculator.calculate();
        if (pairs.isEmpty()) return spark.emptyDataFrame();

        // Build union-find set
        final Map<Long, Long> rowSetMap = FUtils.createUnionFindSet(pairs);

        // Convert union-find set to spark table
        final List<Row> rows = rowSetMap.entrySet().stream().map(e -> RowFactory.create(e.getKey(), e.getValue())).collect(Collectors.toList());
        final StructType structType = new StructType()
                .add(idColumn, DataTypes.LongType, false)
                .add(derivedColumn, DataTypes.LongType, false);
        return spark.createDataFrame(rows, structType).cache();
    }


    public FExternalBinaryModelHandler(SparkSession spark, String idColumn, String externalBinaryModelDerivedColumnSuffix) {
        this.spark = spark;
        this.idColumn = idColumn;
        this.externalBinaryModelDerivedColumnSuffix = externalBinaryModelDerivedColumnSuffix;
    }
}
