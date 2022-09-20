package com.sics.rock.tableinsight4.evidenceset.factory;

import com.sics.rock.tableinsight4.evidenceset.FEmptyEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.FRddEvidenceSet;
import com.sics.rock.tableinsight4.evidenceset.predicateset.*;
import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FLocalPLIUtils;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.iface.FIBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Binary-line evidence set factory
 * <p>
 * The writer has to admit the complication of these codes
 */
public class FBinaryLineEvidenceSetFactory implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FBinaryLineEvidenceSetFactory.class);

    private transient final JavaSparkContext sc;

    private final int evidenceSetPartitionNumber;

    private final boolean positiveNegativeExampleSwitch;

    private final int positiveNegativeExampleNumber;

    private static final Map<Class<? extends FIPredicate>, Integer> PREDICATE_TYPE_MAP = new HashMap<>();


    public FIEvidenceSet createSingleTableBinaryLineEvidenceSet(
            FTableInfo tableInfo, FPLI PLI, FPredicateIndexer predicates, long tableLength) {
        return create(tableInfo, tableInfo, PLI, predicates, tableLength, tableLength, true);
    }

    public FIEvidenceSet createBinaryTableBinaryLineEvidenceSet(
            FTableInfo leftTableInfo, FTableInfo rightTableInfo, FPLI PLI,
            FPredicateIndexer predicates, long leftTableLength, long rightTableLength) {
        return create(leftTableInfo, rightTableInfo, PLI, predicates, leftTableLength, rightTableLength, false);
    }

    @SuppressWarnings("unchecked")
    private FIEvidenceSet create(
            FTableInfo leftTableInfo, FTableInfo rightTableInfo, FPLI PLI,
            FPredicateIndexer predicates, long leftTableLength, long rightTableLength, boolean sameTableFlag) {
        final Broadcast<FPredicateIndexer> predicatesBroadcast = sc.broadcast(predicates);
        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> leftPLI = PLI.getTablePLI(leftTableInfo);
        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> rightPLI = PLI.getTablePLI(rightTableInfo);

        final int numPartitions = evidenceSetRDDPartitionNumber(leftTableLength, rightTableLength, sameTableFlag);

        // a list holds ES in section. The binary-line ES is constructed in cartesian product.
        // However the cartesian product between RDDs conducted by spark is OOM inclined. (i.e. leftPLI.cartesian(rightPLI))
        // A manual cartesian product is coded below
        final List<JavaPairRDD<FBitSet, FIPredicateSet>> localESRDDList =
                IntStream.range(0, leftPLI.getNumPartitions()).parallel().mapToObj(leftPartitionId -> {
                    final List<Tuple2<FPartitionId, Map<FColumnName, FLocalPLI>>> _singleList = leftPLI.filter(t -> t._1.value == leftPartitionId).collect();
                    FAssertUtils.require(() -> _singleList.size() == 1, () -> "Wrong Code!");
                    final FPartitionId leftLocalPLIPartitionId = _singleList.get(0)._1;
                    final Map<FColumnName, FLocalPLI> leftLocalPLI = _singleList.get(0)._2;
                    if (leftLocalPLI.isEmpty()) return null;
                    final Broadcast<Map<FColumnName, FLocalPLI>> leftLocalPLIBC = sc.broadcast(leftLocalPLI);

                    JavaPairRDD<FBitSet, FIPredicateSet> localESRDD = rightPLI.mapPartitionsToPair(_singleIter -> {
                        FAssertUtils.require(_singleIter.hasNext(), "PLI RDD contains empty partition. Code bug?");
                        final Tuple2<FPartitionId, Map<FColumnName, FLocalPLI>> rightLocalPLITuple = _singleIter.next();
                        FPartitionId rightPartitionId = rightLocalPLITuple._1;
                        FAssertUtils.require(!_singleIter.hasNext(), () -> "One PLI RDD partition contains more than one element. " +
                                "The record partition-ids of the first two elements are " + rightLocalPLITuple._1 + " and " + _singleIter.next()._1 + " respectively.");

                        logger.debug("binary line local ES builds of partition-{}-{}", leftLocalPLIPartitionId, rightPartitionId);

                        final Map<FColumnName, FLocalPLI> rightLocalPLI = rightLocalPLITuple._2;
                        if (rightLocalPLI.isEmpty()) return Collections.emptyIterator();
                        final Map<FColumnName, FLocalPLI> leftLocalPLIEX = leftLocalPLIBC.getValue();
                        final FPredicateIndexer allPredicates = predicatesBroadcast.getValue();

                        final int leftMaxRowId = leftLocalPLIEX.values().stream().mapToInt(FLocalPLI::getMaxLocalRowId).max().orElse(-1);
                        final int rightMaxRowId = rightLocalPLI.values().stream().mapToInt(FLocalPLI::getMaxLocalRowId).max().orElse(-1);

                        final FIPredicateSet[] localES = createBinaryLineLocalES(allPredicates, leftLocalPLIEX, rightLocalPLI,
                                leftLocalPLIPartitionId.value, rightPartitionId.value, leftMaxRowId + 1, rightMaxRowId + 1, sameTableFlag);
                        return Arrays.stream(localES).filter(Objects::nonNull).map(ps -> new Tuple2<>(ps.getBitSet(), ps)).iterator();
                    })
                            .reduceByKey(predicateSetMerger(), numPartitions)
                            .persist(StorageLevel.MEMORY_AND_DISK())
                            .setName("b_section_es_" + leftLocalPLIPartitionId + "_" + leftTableInfo.getTableName() + "_" + rightTableInfo.getTableName());
                    // compute ahead otherwise the spark will fail in reduction phase.
                    logger.info("Binary-line local es count = {}", localESRDD.count());
                    return localESRDD;

                }).collect(Collectors.toList());
        if (localESRDDList.isEmpty()) return FEmptyEvidenceSet.getInstance();

        final JavaRDD<FIPredicateSet> es = FTiUtils.mergeReduce(localESRDDList, (r1, r2) ->
                r1.union(r2).reduceByKey(predicateSetMerger(), numPartitions))
                .orElse(JavaPairRDD.fromJavaRDD((JavaRDD<Tuple2<FBitSet, FIPredicateSet>>) (JavaRDD) sc.emptyRDD()))
                .map(Tuple2::_2)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .setName("b_es_" + leftTableInfo.getTableName() + "_" + rightTableInfo.getTableName());

        final FIEvidenceSet evidenceSet = new FRddEvidenceSet(es, sc, predicates.size(), sameTableFlag ? leftTableLength * (leftTableLength - 1) : leftTableLength * rightTableLength);
        logger.info("### Table {}-{} binary line ES built. cardinality is {}", leftTableInfo.getTableName(), rightTableInfo.getTableName(), evidenceSet.cardinality());
        return evidenceSet;
    }


    private FIPredicateSet[] createBinaryLineLocalES(
            FPredicateIndexer allPredicates,
            Map<FColumnName, FLocalPLI> leftPLISection, Map<FColumnName, FLocalPLI> rightPLISection,
            int leftPartitionId, int rightPartitionId,
            int leftRowSize, int rightRowSize, boolean sameTableFlag
    ) {
        final FIPredicateSet[] localES = new FIPredicateSet[leftRowSize * rightRowSize];
        final int predicateSize = allPredicates.size();
        for (int predicateId = 0; predicateId < predicateSize; predicateId++) {
            final FIPredicate predicate = allPredicates.getPredicate(predicateId);
            switch (PREDICATE_TYPE_MAP.getOrDefault(predicate.getClass(), -1)) {
                case FBinaryPredicateID:
                case FBinaryModelPredicateID:
                    doBinaryPredicate((FIBinaryPredicate) predicate, localES, leftPLISection, rightPLISection,
                            leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId);
                    break;
                case FUnaryConsPredicateID:
                    doUnaryConsPredicate((FUnaryConsPredicate) predicate, localES, leftPLISection, rightPLISection,
                            rightRowSize, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId);
                    break;
                case FUnaryIntervalConsPredicateID:
                    doFUnaryIntervalConsPredicate((FUnaryIntervalConsPredicate) predicate, localES, leftPLISection, rightPLISection,
                            rightRowSize, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId);
                    break;
                case FBinaryConsPredicateID:
                case FBinaryIntervalPredicateID:
                    doBinaryConsPredicate(localES, predicateId, allPredicates);
                    break;
                default:
                    throw new RuntimeException("Unknown predicate type " + predicate + " in binary-line ES construction.");
            }
        }


        if (sameTableFlag) {
            if (leftPartitionId == rightPartitionId) {
                for (int localRowId = 0; localRowId < Math.max(leftRowSize, rightRowSize); localRowId++) {
                    final int rowId = localRowId + localRowId * leftRowSize;
                    if (rowId >= localES.length) break;
                    localES[rowId] = null;
                }
            }
        }

        return localES;
    }

    private void doUnaryConsPredicate(FUnaryConsPredicate predicate, FIPredicateSet[] localES,
                                      Map<FColumnName, FLocalPLI> leftPLISection,
                                      Map<FColumnName, FLocalPLI> rightPLISection,
                                      int rightRowSize, int leftRowSize,
                                      int predicateSize, int predicateId,
                                      int leftPartitionId, int rightPartitionId) {
        final long constantIndex = predicate.constant().getIndex();
        final FColumnName columnName = new FColumnName(predicate.columnName());
        final boolean left = predicate.tupleIndex() == 0;
        final FOperator operator = predicate.operator();

        if (left) {
            final FLocalPLI leftLocalPLI = leftPLISection.get(columnName);
            if (FAssertUtils.ASSERT) leftLocalPLI.checkPartitionId(leftPartitionId);

            leftLocalPLI.localRowIdsOf(constantIndex, operator).forEach(leftLocalRowId -> {
                for (int rightLocalRowId = 0; rightLocalRowId < rightRowSize; rightLocalRowId++) {
                    fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                }
            });
        } else {
            final FLocalPLI rightLocalPLI = rightPLISection.get(columnName);
            if (FAssertUtils.ASSERT) rightLocalPLI.checkPartitionId(rightPartitionId);

            rightLocalPLI.localRowIdsOf(constantIndex, operator).forEach(rightLocalRowId -> {
                for (int leftLocalRowId = 0; leftLocalRowId < leftRowSize; leftLocalRowId++) {
                    fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                }
            });
        }
    }

    private void doBinaryConsPredicate(final FIPredicateSet[] localES, final int predicateId,
                                       FPredicateIndexer predicateIndexer) {
        // create by its unary const predicates
        final FPair<Integer, Integer> divideConsPredicates = predicateIndexer.getDivideConsPred(predicateId);
        final int leftConstPredicate = divideConsPredicates._k;
        final int rightConstPredicate = divideConsPredicates._v;
        for (final FIPredicateSet ps : localES) {
            if (ps != null) {
                final FBitSet bitSet = ps.getBitSet();
                if (bitSet.get(leftConstPredicate) && bitSet.get(rightConstPredicate)) {
                    bitSet.set(predicateId);
                }
            }
        }
    }

    private void doFUnaryIntervalConsPredicate(FUnaryIntervalConsPredicate predicate, FIPredicateSet[] localES,
                                               Map<FColumnName, FLocalPLI> leftPLISection,
                                               Map<FColumnName, FLocalPLI> rightPLISection,
                                               int rightRowSize, int leftRowSize,
                                               int predicateSize, int predicateId,
                                               int leftPartitionId, int rightPartitionId) {
        final FInterval interval = predicate.interval();
        if (FAssertUtils.ASSERT) interval.validate();
        final Optional<FConstant<?>> intervalLeft = interval.left();
        final Optional<FConstant<?>> intervalRight = interval.right();

        final boolean left = predicate.tupleIndex() == 0;
        final FColumnName columnName = new FColumnName(predicate.columnName());

        if (left) {
            final FLocalPLI leftLocalPLI = leftPLISection.get(columnName);
            if (FAssertUtils.ASSERT) leftLocalPLI.checkPartitionId(leftPartitionId);
            if (!intervalLeft.isPresent()) {
                leftLocalPLI.localRowIdsOf(intervalRight.get().getIndex(), interval.rightOperator()).forEach(leftLocalRowId -> {
                    for (int rightLocalRowId = 0; rightLocalRowId < rightRowSize; rightLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            } else if (!intervalRight.isPresent()) {
                leftLocalPLI.localRowIdsOf(intervalLeft.get().getIndex(), interval.leftOperator()).forEach(leftLocalRowId -> {
                    for (int rightLocalRowId = 0; rightLocalRowId < rightRowSize; rightLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            } else {
                leftLocalPLI.localRowIdsBetween(intervalLeft.get().getIndex(), intervalRight.get().getIndex(),
                        interval.leftClose(), interval.rightClose()).forEach(leftLocalRowId -> {
                    for (int rightLocalRowId = 0; rightLocalRowId < rightRowSize; rightLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            }
        } else { // right, tuple-id = 1
            final FLocalPLI rightLocalPLI = rightPLISection.get(columnName);
            if (FAssertUtils.ASSERT) rightLocalPLI.checkPartitionId(rightPartitionId);
            if (!intervalLeft.isPresent()) {
                rightLocalPLI.localRowIdsOf(intervalRight.get().getIndex(), interval.rightOperator()).forEach(rightLocalRowId -> {
                    for (int leftLocalRowId = 0; leftLocalRowId < leftRowSize; leftLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            } else if (!intervalRight.isPresent()) {
                rightLocalPLI.localRowIdsOf(intervalLeft.get().getIndex(), interval.leftOperator()).forEach(rightLocalRowId -> {
                    for (int leftLocalRowId = 0; leftLocalRowId < leftRowSize; leftLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            } else {
                rightLocalPLI.localRowIdsBetween(intervalLeft.get().getIndex(), intervalRight.get().getIndex(),
                        interval.leftClose(), interval.rightClose()).forEach(rightLocalRowId -> {
                    for (int leftLocalRowId = 0; leftLocalRowId < leftRowSize; leftLocalRowId++) {
                        fillLocalES(localES, leftRowSize, predicateSize, predicateId, leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId);
                    }
                });
            }
        }
    }

    private void doBinaryPredicate(FIBinaryPredicate predicate, FIPredicateSet[] localES,
                                   Map<FColumnName, FLocalPLI> leftPLISection,
                                   Map<FColumnName, FLocalPLI> rightPLISection,
                                   int leftRowSize, int predicateSize, int predicateId,
                                   int leftPartitionId, int rightPartitionId) {
        final FColumnName leftCol = new FColumnName(predicate.leftCol());
        final FColumnName rightCol = new FColumnName(predicate.rightCol());
        final FOperator operator = predicate.operator();

        // not null since filtered
        final FLocalPLI leftPLI = leftPLISection.get(leftCol);
        final FLocalPLI rightPLI = rightPLISection.get(rightCol);

        if (FAssertUtils.ASSERT) leftPLI.checkPartitionId(leftPartitionId);
        if (FAssertUtils.ASSERT) rightPLI.checkPartitionId(rightPartitionId);

        FLocalPLIUtils.localRowIdsOf(leftPLI, rightPLI, operator).forEach(leftRightRowIds -> {
            List<Integer> leftLocalRowIds = leftRightRowIds._k;
            Stream<Integer> rightLocalRowIds = leftRightRowIds._v;
            rightLocalRowIds.forEach(rightLocalRowId ->
                    leftLocalRowIds.forEach(leftLocalRowId ->
                            fillLocalES(localES, leftRowSize, predicateSize, predicateId,
                                    leftPartitionId, rightPartitionId, leftLocalRowId, rightLocalRowId)
                    )
            );
        });
    }

    // inline
    private void fillLocalES(FIPredicateSet[] localES, int leftRowSize,
                             int predicateSize, int predicateId,
                             int leftPartitionId, int rightPartitionId,
                             int leftLocalRowId, int rightLocalRowId) {
        final int tupleId = leftLocalRowId + rightLocalRowId * leftRowSize;
        FIPredicateSet ps = localES[tupleId];
        if (ps == null) {
            if (positiveNegativeExampleSwitch) {
                ps = new FExamplePredicateSet(new FBitSet(predicateSize), 1L);
                // record left and right partitionId:offset for example
                ((FExamplePredicateSet) ps).add(new FRddElementIndex[]{
                        new FRddElementIndex(leftPartitionId, leftLocalRowId),
                        new FRddElementIndex(rightPartitionId, rightLocalRowId)
                });
            } else {
                ps = new FPredicateSet(new FBitSet(predicateSize), 1L);
            }
            localES[tupleId] = ps;
        }
        ps.getBitSet().set(predicateId);
    }

    private int evidenceSetRDDPartitionNumber(long leftTableLength, long rightTableLength, boolean sameTableFlag) {
        final int pn;
        if (this.evidenceSetPartitionNumber == -1) {
            final long tupleSize = sameTableFlag ? leftTableLength * (leftTableLength - 1) : leftTableLength * rightTableLength;
            // a section holds 1m tuple-pair
            final int sectionNumber = (int) (tupleSize >> 20);
            pn = Math.max(sc.defaultParallelism(), sectionNumber);
        } else {
            pn = this.evidenceSetPartitionNumber;
        }
        logger.info("Binary line evidence set RDD partition number {}", pn);
        return pn;
    }

    private Function2<FIPredicateSet, FIPredicateSet, FIPredicateSet> predicateSetMerger() {
        return this.positiveNegativeExampleSwitch ? new FExamplePredicateSetMerger(this.positiveNegativeExampleNumber) : FPredicateSetMerger.instance;
    }

    public FBinaryLineEvidenceSetFactory(SparkSession spark, int evidenceSetPartitionNumber,
                                         boolean positiveNegativeExampleSwitch, int positiveNegativeExampleNumber) {
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.positiveNegativeExampleSwitch = positiveNegativeExampleSwitch;
        this.positiveNegativeExampleNumber = positiveNegativeExampleNumber;
        this.evidenceSetPartitionNumber = evidenceSetPartitionNumber;
    }


    private static final int FBinaryPredicateID = 1;
    private static final int FBinaryModelPredicateID = 2;
    private static final int FUnaryConsPredicateID = 3;
    private static final int FUnaryIntervalConsPredicateID = 4;
    private static final int FBinaryConsPredicateID = 5;
    private static final int FBinaryIntervalPredicateID = 6;

    static {
        PREDICATE_TYPE_MAP.put(FBinaryPredicate.class, FBinaryPredicateID);
        PREDICATE_TYPE_MAP.put(FBinaryModelPredicate.class, FBinaryModelPredicateID);
        PREDICATE_TYPE_MAP.put(FUnaryConsPredicate.class, FUnaryConsPredicateID);
        PREDICATE_TYPE_MAP.put(FUnaryIntervalConsPredicate.class, FUnaryIntervalConsPredicateID);
        PREDICATE_TYPE_MAP.put(FBinaryConsPredicate.class, FBinaryConsPredicateID);
        PREDICATE_TYPE_MAP.put(FBinaryIntervalConsPredicate.class, FBinaryIntervalPredicateID);

    }
}
