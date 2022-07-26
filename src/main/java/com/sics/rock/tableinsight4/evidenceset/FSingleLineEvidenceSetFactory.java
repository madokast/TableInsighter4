package com.sics.rock.tableinsight4.evidenceset;

import com.sics.rock.tableinsight4.evidenceset.predicateset.*;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.FPredicateFactory;
import com.sics.rock.tableinsight4.predicate.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.FUnaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.iface.FIUnaryPredicate;
import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;


public class FSingleLineEvidenceSetFactory implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FSingleLineEvidenceSetFactory.class);

    private transient final JavaSparkContext sc;

    private final int evidenceSetPartitionNumber;

    private final boolean positiveNegativeExampleSwitch;

    private final int positiveNegativeExampleNumber;

    public FIEvidenceSet singleLineEvidenceSet(FTableInfo tableInfo, FPLI PLI, FPredicateFactory singleLinePredicates, long tableLength) {

        Broadcast<FPredicateFactory> predicatesBroadcast = sc.broadcast(singleLinePredicates);

        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> tablePLI = PLI.getTablePLI(tableInfo);

        JavaRDD<FIPredicateSet> esRDD = tablePLI.mapPartitionsToPair(singleIter -> {
            // Only one element in one partition!
            FAssertUtils.require(singleIter.hasNext(), "PLI RDD contains empty partition. Code bug?");
            final Tuple2<FPartitionId, Map<FColumnName, FLocalPLI>> localPLITuple = singleIter.next();
            FAssertUtils.require(!singleIter.hasNext(), () -> "One PLI RDD partition contains more than one element. " +
                    "The record partition-ids of the first two elements are " + localPLITuple._1 + " and " + singleIter.next()._1 + " respectively.");

            logger.info("single line local ES builds of partition-{}", localPLITuple._1);
            final Map<FColumnName, FLocalPLI> colPLIMap = localPLITuple._2;
            if (colPLIMap.isEmpty()) return Collections.emptyIterator();

            // calculate max local row-id
            final int maxRowId = colPLIMap.values().stream().mapToInt(FLocalPLI::getMaxLocalRowId).max().orElse(-1);
            // bug-fix: maxRowId == 0 does not mean empty
            if (maxRowId == -1) return Collections.emptyIterator();

            // build local ES
            final FIPredicateSet[] localES = createSingleLineLocalES(predicatesBroadcast.getValue(), colPLIMap, maxRowId + 1);
            // as stream
            return Arrays.stream(localES).filter(Objects::nonNull).map(ps -> new Tuple2<>(ps.getBitSet(), ps)).iterator();

        })
                // reduce depend on positiveNegativeExampleSwitch
                .reduceByKey(this.positiveNegativeExampleSwitch ? new FExamplePredicateSetMerge(positiveNegativeExampleNumber) : FPredicateSetMerge.instance,
                        evidenceSetPartitionNumber == -1 ? ((int) (tableLength / 1000)) + 1 : evidenceSetPartitionNumber)
                .map(t -> t._2)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .setName("single_line_es_" + tableInfo.getTableName());

        FRddEvidenceSet ES = new FRddEvidenceSet(esRDD, sc, singleLinePredicates.size(), tableLength);
        logger.info("### Table {} single line ES built. allCount is {}", tableInfo.getTableName(), ES.allCount());

        return ES;
    }

    private FIPredicateSet[] createSingleLineLocalES(FPredicateFactory predicates, Map<FColumnName, FLocalPLI> colPLIMap, int rowSize) {
        final FIPredicateSet[] localES = new FIPredicateSet[rowSize];
        final int predicateSize = predicates.size();
        for (int predicateId = 0; predicateId < predicateSize; predicateId++) {
            final FIUnaryPredicate predicate = (FIUnaryPredicate) predicates.getPredicate(predicateId);
            logger.info("local ES deals with {}", predicate);
            final String columnName = predicate.columnName();
            final FLocalPLI localPLI = colPLIMap.get(new FColumnName(columnName));
            final int partitionId = localPLI.getPartitionId();

            final FOperator operator = predicate.operator();
            if (predicate instanceof FUnaryConsPredicate) {
                final FConstant<?> constant = ((FUnaryConsPredicate) predicate).constant();
                final long index = constant.getIndex();
                localPLI.localRowIdsOf(index, operator).forEach(fillPredicateSet(localES, predicateSize, predicateId, partitionId));
            } else if (predicate instanceof FUnaryIntervalConsPredicate) {
                final FInterval interval = ((FUnaryIntervalConsPredicate) predicate).interval();
                final Optional<FConstant<Double>> left = interval.left();
                final Optional<FConstant<Double>> right = interval.right();
                if (!left.isPresent()) {
                    FAssertUtils.require(right::isPresent, () -> "The interval of predicate " + predicate + " has no bound.");
                    localPLI.localRowIdsOf(right.get().getIndex(), interval.leftClose() ? FOperator.LET : FOperator.LT)
                            .forEach(fillPredicateSet(localES, predicateSize, predicateId, partitionId));
                } else if (!right.isPresent()) {
                    localPLI.localRowIdsOf(left.get().getIndex(), interval.leftClose() ? FOperator.GET : FOperator.GT)
                            .forEach(fillPredicateSet(localES, predicateSize, predicateId, partitionId));
                } else {
                    localPLI.localRowIdsBetween(left.get().getIndex(), right.get().getIndex(), interval.leftClose(), interval.rightClose())
                            .forEach(fillPredicateSet(localES, predicateSize, predicateId, partitionId));
                }
            } else {
                throw new RuntimeException("Unknown predicate type " + predicate);
            }
        }

        return localES;
    }

    /**
     * fill the localES
     */
    // @inline!!!
    private Consumer<Integer> fillPredicateSet(FIPredicateSet[] localES, int predicateSize, int predicateId, int partitionId) {
        return localRowId -> {
            FIPredicateSet ps = localES[localRowId];
            if (ps == null) {
                if (positiveNegativeExampleSwitch) {
                    ps = new FExamplePredicateSet(new FBitSet(predicateSize), 1L);
                    // record partitionId:offset for example
                    ((FExamplePredicateSet) ps).add(new FRddElementIndex[]{new FRddElementIndex(partitionId, localRowId)});
                } else {
                    ps = new FPredicateSet(new FBitSet(predicateSize), 1L);
                }
                localES[localRowId] = ps;
            }
            ps.getBitSet().set(predicateId);
        };
    }


    public FSingleLineEvidenceSetFactory(SparkSession spark, int evidenceSetPartitionNumber,
                                         boolean positiveNegativeExampleSwitch, int positiveNegativeExampleNumber) {
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        this.positiveNegativeExampleSwitch = positiveNegativeExampleSwitch;
        this.positiveNegativeExampleNumber = positiveNegativeExampleNumber;
        this.evidenceSetPartitionNumber = evidenceSetPartitionNumber;
    }
}
