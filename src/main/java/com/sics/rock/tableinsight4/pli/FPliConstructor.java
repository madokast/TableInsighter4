package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.partitioner.FOrderedPartitioner;
import com.sics.rock.tableinsight4.internal.partitioner.FPartitionIdPartitioner;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Create FLocalPLI (Position List Indexes) fro each column
 *
 * @author zhaorx
 */
public class FPliConstructor {

    private static final Logger logger = LoggerFactory.getLogger(FPliConstructor.class);

    private final String idColName;

    // 1000-2500?
    private final int sliceLengthForPLI;

    private final boolean positiveNegativeExampleSwitch;

    private final SparkSession spark;

    private final JavaSparkContext sc;

    public FPLI construct(FTableDatasetMap tableDatasetMap) {
        // 1. Use FOrderedPartitioner to distribute table data into partitions of same capacity.
        //    So each row is ordered globally in order to find the positive / negative examples of rules.
        partitionAllTable(tableDatasetMap);

        // 2. Create type-based, unique and ordered index of all the value in table data.
        final FTypedOrderedIndex typedOrderedIndex = createTypedOrderedIndex(tableDatasetMap);

        // 3. Fill the index of constant value.
        fillIndexOfConstant(tableDatasetMap, typedOrderedIndex);

        // 4. Create PLI
        //    The elementIndex -> RowId Map/RDD is also constructed
        FPLI PLI = createPLI(tableDatasetMap, typedOrderedIndex);

        if (logger.isDebugEnabled()) {
            printPLI(tableDatasetMap, typedOrderedIndex, PLI);
        }

        return PLI;
    }

    /**
     * Partition the table by FOrderedPartitioner
     */
    private void partitionAllTable(FTableDatasetMap tableDatasetMap) {
        tableDatasetMap.foreach((tableInfo, dataset) -> {
            final JavaRDD<Row> originRDD = dataset.toJavaRDD();
            final JavaPairRDD<Long, Row> orderedRDD = FSparkUtils.addOrderedId(originRDD);
            final long length = tableInfo.getLength(orderedRDD::count);
            final FOrderedPartitioner orderedPartitioner = new FOrderedPartitioner(length, sliceLengthForPLI);
            logger.info("### Table {}, length {}, shuffle to {} partitions. SliceLengthForPLI = {}", tableInfo.getTableName(), length,
                    orderedPartitioner.numPartitions(), sliceLengthForPLI);

            final JavaRDD<Row> slicedRDD = orderedRDD.partitionBy(orderedPartitioner).map(Tuple2::_2)
                    // TODO cache here reports Block rdd_24_x already exists on this machine; not re-adding it
                    .cache().setName(tableInfo.getTableName() + "_SLICED");
            final Dataset<Row> slicedDataset = spark.createDataFrame(slicedRDD, dataset.schema());
            tableDatasetMap.updateDataset(tableInfo, slicedDataset);
        });
    }

    /**
     * Create ordered (if comparable) index of each type
     *
     * @return map valueType -> index
     */
    private FTypedOrderedIndex createTypedOrderedIndex(FTableDatasetMap tableDatasetMap) {
        // select distinct each column as rdd
        // then group by valueType
        final Map<FValueType, List<JavaRDD<Object>>> typeBasedRddList = new HashMap<>();
        tableDatasetMap.foreach((tableInfo, dataset) -> {
            tableInfo.nonSkipColumnsView().forEach(columnInfo -> {
                final String columnName = columnInfo.getColumnName();
                final FValueType valueType = columnInfo.getValueType();
                // rdd of column values
                final JavaRDD<Object> distinctRDD = dataset.select(columnName).toJavaRDD().map(row -> row.get(0))
                        .filter(Objects::nonNull).distinct();
                // rdd of constant in this column
                final JavaRDD<Object> constantValRDD = FSparkUtils.rddOf(columnInfo.getConstants().stream().map(FConstant::getConstant)
                        .filter(Objects::nonNull).collect(Collectors.toList()), spark);
                // rdd of interval constant in this column
                final JavaRDD<Object> intervalConstantValRDD = FSparkUtils.rddOf(columnInfo.getIntervalConstants().stream()
                        .flatMap(i -> i.constants().stream().map(FConstant::getConstant))
                        .filter(d -> !d.isInfinite()).collect(Collectors.toList()), spark);

                JavaRDD<Object> allData = FSparkUtils.union(spark, FTiUtils.listOf(distinctRDD, constantValRDD, intervalConstantValRDD));

                typeBasedRddList.putIfAbsent(valueType, new ArrayList<>());
                typeBasedRddList.get(valueType).add(allData);
            });
        });

        // union all rddList belonging to its type
        // re distinct it and sort it if comparable
        // then create ordered index
        Map<FValueType, JavaPairRDD<Object, Long>> indexMap = typeBasedRddList.entrySet().stream().map(e -> {
            final FValueType type = e.getKey();
            final List<JavaRDD<Object>> RDDs = e.getValue();
            JavaRDD<Object> distinctRDD = FSparkUtils.union(spark, RDDs).distinct();
            if (type.isComparable()) {
                distinctRDD = distinctRDD.sortBy(v -> v, true, distinctRDD.getNumPartitions());
            }
            final JavaPairRDD<Object, Long> indexRdd = FSparkUtils.orderedIndex(distinctRDD).cache().setName("Index_of_" + type);
            return new FPair<>(type, indexRdd);
        }).collect(Collectors.toMap(FPair::k, FPair::v));

        return new FTypedOrderedIndex(indexMap);
    }


    private void fillIndexOfConstant(FTableDatasetMap tableDatasetMap, FTypedOrderedIndex typedOrderedIndex) {
        // 1. find all constants and group by its type
        final Map<FValueType, Set<Object>> typeBasedConstants = new HashMap<>();
        tableDatasetMap.foreach((tabInfo, ignore) -> {
            tabInfo.getColumns().forEach(columnInfo -> {
                final FValueType valueType = columnInfo.getValueType();
                final List<Object> constants = columnInfo.getConstants().stream().map(FConstant::getConstant)
                        .filter(Objects::nonNull).collect(Collectors.toList());
                typeBasedConstants.putIfAbsent(valueType, new HashSet<>());
                typeBasedConstants.get(valueType).addAll(constants);
            });
        });

        // 2. find index of constants by left join index-map
        Map<FValueType, Map<Object, Long>> typeBasedConstantIndexMap = typeBasedConstants
                .entrySet().stream().map(e -> {
                    final FValueType valueType = e.getKey();
                    final Set<Object> constant = e.getValue();
                    Map<Object, Long> constantIndexMap = typedOrderedIndex.indexesOf(valueType).map(index ->
                            sc.parallelize(new ArrayList<>(constant))
                                    // convert to tuple for join operator
                                    .mapToPair(cons -> new Tuple2<>(cons, null))
                                    .leftOuterJoin(index)
                                    // after left join, the key is constant and the value is [null, opt[index]]
                                    .mapToPair(t -> new Tuple2<>(t._1, t._2._2.or(FConstant.INDEX_NOT_FOUND)))
                                    .collect()
                                    .stream()
                                    .collect(Collectors.toMap(t -> t._1, t -> t._2))).orElse(Collections.emptyMap()
                    );
                    return new FPair<>(valueType, constantIndexMap);
                }).collect(Collectors.toMap(FPair::k, FPair::v));

        // 3. fill the index of constant. Note that the 2 exceptions: index of null, and index not-found
        tableDatasetMap.foreach((tabInfo, ignore) ->
                tabInfo.getColumns().forEach(columnInfo -> {
                    final FValueType valueType = columnInfo.getValueType();

                    // fill constant
                    columnInfo.getConstants().forEach(cons -> {
                        final Object consVal = cons.getConstant();
                        if (consVal == null) {
                            cons.setIndex(FConstant.INDEX_OF_NULL);
                        } else {
                            long index = typeBasedConstantIndexMap.getOrDefault(valueType, Collections.emptyMap())
                                    .getOrDefault(consVal, FConstant.INDEX_NOT_FOUND);
                            cons.setIndex(index);
                            if (index == FConstant.INDEX_NOT_FOUND) {
                                logger.error("Column {}.{} contains constant {}, but cannot found its index",
                                        tabInfo.getTableName(), columnInfo.getColumnName(), consVal);
                            }
                        }
                    });

                    // fill interval
                    columnInfo.getIntervalConstants().forEach(interval -> {
                        interval.right().ifPresent(cons-> {
                            Double consVal = cons.getConstant();
                            long index = typeBasedConstantIndexMap.getOrDefault(valueType, Collections.emptyMap())
                                    .getOrDefault(consVal, FConstant.INDEX_NOT_FOUND);
                            cons.setIndex(index);
                            logger.error("Column {}.{} contains interval constant {}, but cannot found its index",
                                    tabInfo.getTableName(), columnInfo.getColumnName(), consVal);
                        });
                        interval.left().ifPresent(cons-> {
                            Double consVal = cons.getConstant();
                            long index = typeBasedConstantIndexMap.getOrDefault(valueType, Collections.emptyMap())
                                    .getOrDefault(consVal, FConstant.INDEX_NOT_FOUND);
                            cons.setIndex(index);
                            logger.error("Column {}.{} contains interval constant {}, but cannot found its index",
                                    tabInfo.getTableName(), columnInfo.getColumnName(), consVal);
                        });
                    });
                }));


    }

    private JavaPairRDD<Row, FRddElementIndex> createEleIndexTabRDD(FTableInfo tableInfo, Dataset<Row> table) {
        final String tableName = tableInfo.getTableName();
        final JavaRDD<Row> tabRdd = table.toJavaRDD();
        return FRddElementIndexUtils.rddElementIndex(tabRdd).cache().setName("Element_index_table_" + tableName);
    }

    /**
     * Create PLI (sliced) for each column
     * <p>
     * Type FPartitionId is just int
     * Type FColumnName is just string
     */
    private JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> createPLI(
            FTableInfo tableInfo,
            StructType schema,
            JavaPairRDD<Row, FRddElementIndex> eleIndexTabRDD,
            FTypedOrderedIndex typedOrderedIndex) {
        Map<String, Integer> column2index = FTiUtils.indexArray(schema.fieldNames());
        final int numPartitions = eleIndexTabRDD.getNumPartitions();
        final String tableName = tableInfo.getTableName();

        // each column
        Map<FColumnName, JavaPairRDD<FPartitionId, FLocalPLI>> col2pid2pli = tableInfo.nonSkipColumnsView().stream().map(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            final Integer columnId = column2index.get(columnName);
            FAssertUtils.require(columnId != null, () -> columnName + " not found in " + Arrays.toString(schema.fieldNames()));
            final FValueType valueType = columnInfo.getValueType();

            JavaPairRDD<FPartitionId, FLocalPLI> thisColPLI = eleIndexTabRDD.mapToPair(rowEleIndex -> {
                final Object value = rowEleIndex._1.get(columnId);
                final FRddElementIndex elementIndex = rowEleIndex._2;
                return new Tuple2<>(value, elementIndex);
            })
                    /*
                     * left join to get the index (rightRdd.value) of value (leftRdd.key)
                     * left : RDD[value, elementIndex]
                     * right: RDD[value, index]
                     * if the value is null, the index is also null. Set it as -1
                     */
                    .leftOuterJoin(typedOrderedIndex.indexesOf(valueType)
                            .orElseThrow(() -> new RuntimeException("The index data of type " + valueType + " does not exist")))
                    // join result is RDD[value, [elementIndex, opt(index)]]
                    .mapToPair(value2indexIndex -> {
                        // final Object value = value2indexIndex._1; // nullable
                        final Tuple2<FRddElementIndex, Optional<Long>> indexIndex = value2indexIndex._2;
                        final FRddElementIndex elementIndex = indexIndex._1;
                        final Long index = indexIndex._2.or(FConstant.INDEX_OF_NULL);

                        final int partitionId = elementIndex.partitionId;
                        final int offset = elementIndex.offset;

                        return new Tuple2<>(FPartitionId.of(partitionId), FLocalPLI.singleLinePLI(partitionId, index, offset));
                    })
                    // in the reduce stage, the rdd is re-partition to the origin
                    .reduceByKey(new FPartitionIdPartitioner(numPartitions), FLocalPLI::merge)
                    .cache()
                    .setName("ORIGIN_PLI_" + tableName + "." + columnName);

            return new FPair<>(new FColumnName(columnName), thisColPLI);
        }).collect(Collectors.toMap(FPair::k, FPair::v));


        JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> pid2col2pli = FSparkUtils.swapKey(col2pid2pli);
        return pid2col2pli.cache().setName("SWAP_KEY_PLI_" + tableName);
    }

    private JavaPairRDD<FRddElementIndex, Long> createElementIndexIDMap(
            StructType schema,
            JavaPairRDD<Row, FRddElementIndex> eleIndexTabRDD) {
        final String[] columns = schema.fieldNames();
        int i = 0;
        while (i < columns.length && !columns[i].equals(idColName)) ++i;
        final int finalI = i;
        FAssertUtils.require(() -> columns[finalI].equals(idColName), () -> idColName + " not found in " + Arrays.toString(columns));
        return eleIndexTabRDD.mapToPair(t -> new Tuple2<>(t._2, (Long) t._1.get(finalI)));
    }

    private FPLI createPLI(FTableDatasetMap tableDatasetMap, FTypedOrderedIndex typedOrderedIndex) {
        FPLI PLI = new FPLI();
        tableDatasetMap.foreach((tableInfo, dataset) -> {
            final String tableName = tableInfo.getTableName();
            final StructType schema = dataset.schema();
            final JavaPairRDD<Row, FRddElementIndex> eleIndexTabRDD = FRddElementIndexUtils
                    .rddElementIndex(dataset.toJavaRDD()).cache().setName("Element_index_table_" + tableName);
            final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> pliOfTable = createPLI(tableInfo, schema, eleIndexTabRDD, typedOrderedIndex);

            PLI.putTablePLI(tableInfo, pliOfTable);
            if (positiveNegativeExampleSwitch) {
                final JavaPairRDD<FRddElementIndex, Long> elementIndexIDMap = createElementIndexIDMap(schema, eleIndexTabRDD);
                PLI.putTableIDColumnMap(tableInfo, elementIndexIDMap);
            }
        });

        return PLI;
    }

    /**
     * debug
     */
    private void printPLI(FTableDatasetMap tableDatasetMap, FTypedOrderedIndex typedOrderedIndex, FPLI PLI) {
        tableDatasetMap.foreach((tableInfo, dataset) -> {

            JavaRDD<Row> tableWithEleId = FRddElementIndexUtils.rddElementIndex(dataset.toJavaRDD())
                    .map(t -> {
                        Row row = t._1;
                        FRddElementIndex rddElementIndex = t._2;
                        List<Object> objects = FScalaUtils.seqToJList(row.toSeq());
                        objects.add(rddElementIndex.toString());
                        return RowFactory.create(objects.toArray());
                    });

            String pidOffsetCol = "pid:offset";
            dataset = spark.createDataFrame(tableWithEleId, dataset.schema().add(pidOffsetCol, DataTypes.StringType));

            List<String> selectors = new ArrayList<>();
            selectors.add(pidOffsetCol);

            for (FColumnInfo columnInfo : tableInfo.nonSkipColumnsView()) {
                String columnName = columnInfo.getColumnName();
                FValueType valueType = columnInfo.getValueType();
                JavaRDD<Row> valIndexMap = typedOrderedIndex.indexesOf(valueType).get()
                        .map(t -> RowFactory.create(t._1, t._2));

                StructType structType = new StructType().add(columnName, valueType.getSparkSqlType()).add(columnName + "_id", DataTypes.LongType);
                Dataset<Row> indexTable = spark.createDataFrame(valIndexMap, structType);
                dataset = FSparkSqlUtils.leftOuterJoin(dataset, indexTable, Collections.singletonList(columnName));

                selectors.add(columnName);
                selectors.add(columnName + "_id");
            }

            logger.debug("Index table {}: ", tableInfo.getTableName());
            dataset.select(idColName, FScalaUtils.seqOf(selectors)).orderBy(idColName).show(false);

            JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> tablePLI = PLI.getTablePLI(tableInfo);
            tablePLI.collect().forEach(pidMap -> {
                FPartitionId pid = pidMap._1;
                Map<FColumnName, FLocalPLI> map = pidMap._2;
                map.forEach((col, pli) -> {
                    logger.debug("{} {} in partition id {}", col, pli, pid);
                });
            });

        });
    }

    public FPliConstructor(String idColName, int sliceLengthForPLI, boolean positiveNegativeExampleSwitch, SparkSession spark) {
        this.idColName = idColName;
        this.sliceLengthForPLI = sliceLengthForPLI;
        this.positiveNegativeExampleSwitch = positiveNegativeExampleSwitch;
        this.spark = spark;
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    }
}
