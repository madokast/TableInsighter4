package com.sics.rock.tableinsight4.predicate.factory.impl;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FIPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryModelPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.function.Supplier;


/**
 * create binary-table cross-line (2-line) predicates like tab1.t0.name = tab2.t1.name
 * in default:
 * the binary (2-line) predicates
 * are created
 *
 * @author zhaorx
 */
public class FBinaryTableCrossLinePredicateFactory implements FIPredicateFactory {

    @Override
    public FPredicateIndexer createPredicates() {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();
        final String leftTableName = leftTable.getTableName();
        final String rightTableName = rightTable.getTableName();
        final String leftInnerTableName = leftTable.getInnerTableName();
        final String rightInnerTableName = rightTable.getInnerTableName();

        leftTable.nonSkipColumnsView().forEach(leftColumnInfo -> {
            final String leftColumnName = leftColumnInfo.getColumnName();
            final Supplier<Set<String>> leftInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(leftTableName, leftInnerTableName, leftColumnName);

            rightTable.nonSkipColumnsView().forEach(rightColumnInfo -> {
                // value type should be identical
                if (!leftColumnInfo.getValueType().equals(rightColumnInfo.getValueType())) return;

                final String rightColumnName = rightColumnInfo.getColumnName();
                final Supplier<Set<String>> rightInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(rightTableName, rightInnerTableName, rightColumnName);
                final Supplier<Set<String>> innerTabCols = () -> FTiUtils.setUnion(leftInnerTabCols.get(), rightInnerTabCols.get()); // identifier

                // model derived column
                if (derivedColumnNameHandler.isDerivedBinaryModelColumnName(leftColumnName)) {
                    if (!leftColumnName.equals(rightColumnName)) return;
                    final String leftTable_ = derivedColumnNameHandler.extractModelInfo(leftColumnName).getLeftTableName();
                    final String rightTable_ = derivedColumnNameHandler.extractModelInfo(rightColumnName).getRightTableName();
                    if (FTiUtils.setOf(leftTableName, rightTableName).equals(FTiUtils.setOf(leftTable_, rightTable_))) {
                        predicateIndexer.put(new FBinaryModelPredicate(leftTableName, rightTableName, leftColumnName, innerTabCols.get()));
                    }
                    return;
                }

                // normal column. check similarity
                if (columnSimilarity(leftColumnInfo, rightColumnInfo) >= crossColumnThreshold) {
                    predicateIndexer.put(new FBinaryPredicate(leftTableName, rightTableName, leftColumnName, rightColumnName, FOperator.EQ, innerTabCols.get()));
                }

            }); // end right loop
        }); // end left loop

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        return predicateIndexer;
    }

    private Map<FPair<FTableInfo, FColumnName>, JavaPairRDD<Long, Long>> FREQUENT_TABLE_CACHE = new HashMap<>();

    private double columnSimilarity(final FColumnInfo leftColumnInfo,
                                    final FColumnInfo rightColumnInfo) {
        final FColumnName leftColumnName = new FColumnName(leftColumnInfo.getColumnName());
        final FColumnName rightColumnName = new FColumnName(rightColumnInfo.getColumnName());

        final JavaPairRDD<Long, Long> leftFrequentTable = FREQUENT_TABLE_CACHE.computeIfAbsent(
                new FPair<>(leftTable, leftColumnName), this::findFrequentTable);

        final JavaPairRDD<Long, Long> rightFrequentTable = FREQUENT_TABLE_CACHE.computeIfAbsent(
                new FPair<>(rightTable, rightColumnName), this::findFrequentTable);

        if (leftFrequentTable.count() == 0L || rightFrequentTable.count() == 0L) return 0D;

        final double shareNumber = leftFrequentTable.join(rightFrequentTable)
                .map(Tuple2::_2).map(ff -> Math.min(ff._1, ff._2))
                .fold(0L, Long::sum).doubleValue();
        if (shareNumber == 0L) return 0D;

        final long leftLength = leftTable.getLength(() -> datasetMap.getDatasetByTableName(leftTable.getTableName()).count());
        final long rightLength = rightTable.getLength(() -> datasetMap.getDatasetByTableName(rightTable.getTableName()).count());

        return Math.max(shareNumber / leftLength, shareNumber / rightLength);

    }

    private JavaPairRDD<Long, Long> findFrequentTable(FPair<FTableInfo, FColumnName> tabCol) {
        final FTableInfo tableInfo = tabCol._k;
        final String tableName = tableInfo.getTableName();
        final FColumnName columnName = tabCol._v;
        final JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> tablePLI = PLI.getTablePLI(tableInfo);

        return tablePLI.map(Tuple2::_2)
                .map(m -> m.getOrDefault(columnName, null))
                .filter(Objects::nonNull)
                .flatMap(FLocalPLI::indexRowIdsIterator)
                .mapToPair(idRowIds -> new Tuple2<>(idRowIds.getKey(), (long) idRowIds.getValue().size()))
                .reduceByKey(Long::sum)
                .persist(StorageLevel.MEMORY_AND_DISK())
                .setName("FREQ_" + tableName + "_" + columnName);
    }

    /*===================== materials =====================*/

    private final FTableInfo leftTable;

    private final FTableInfo rightTable;

    private final List<FExternalPredicateInfo> otherInfos;


    /*===================== configs & infos =====================*/

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    private final FTableDatasetMap datasetMap;

    private final FPLI PLI;

    private final double crossColumnThreshold;


    public FBinaryTableCrossLinePredicateFactory(
            final FTableInfo leftTable, final FTableInfo rightTable,
            final List<FExternalPredicateInfo> otherInfos,
            final FDerivedColumnNameHandler derivedColumnNameHandler, final FTableDatasetMap datasetMap,
            final FPLI PLI, final double crossColumnThreshold) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.otherInfos = otherInfos;
        this.derivedColumnNameHandler = derivedColumnNameHandler;
        this.datasetMap = datasetMap;
        this.PLI = PLI;
        this.crossColumnThreshold = crossColumnThreshold;
    }
}
