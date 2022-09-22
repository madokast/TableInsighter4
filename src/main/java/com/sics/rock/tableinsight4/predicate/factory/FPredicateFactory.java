package com.sics.rock.tableinsight4.predicate.factory;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.function.Supplier;

/**
 * predicate factory
 *
 * @author zhaorx
 */
public class FPredicateFactory {

    private static final Logger logger = LoggerFactory.getLogger(FPredicateFactory.class);

    /**
     * create single-line predicate like t0.name = aaa
     * the unary (single-line) constant predicates and unary (single-line) interval constant predicates will be created in default
     */
    public static FPredicateIndexer createSingleLinePredicates(
            FTableInfo table, FDerivedColumnNameHandler derivedColumnNameHandler, List<FExternalPredicateInfo> otherInfos) {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();

        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName, columnName);
            columnInfo.getConstants().stream().map(cons ->
                    new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols)
            ).forEach(predicateIndexer::put);
            columnInfo.getIntervalConstants().stream().map(interval ->
                    new FUnaryIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols)
            ).forEach(predicateIndexer::put);
        });

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        return predicateIndexer;
    }

    /**
     * create single-table cross-line (2-line) predicates like t0.name = t1.name
     * in default:
     * the unary (single-line) constant predicates
     * the unary (single-line) interval constant predicates
     * the binary (2-line) predicates
     * are created
     */
    public static FPredicateIndexer createSingleTableCrossLinePredicates(
            FTableInfo table, boolean constantPredicateFlag, FDerivedColumnNameHandler derivedColumnNameHandler,
            List<FExternalPredicateInfo> otherInfos) {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();
        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName, columnName);

            if (constantPredicateFlag) {
                columnInfo.getConstants().forEach(cons -> {
                    FIPredicate t0 = new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t1 = new FUnaryConsPredicate(tabName, columnName, 1, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t01 = new FBinaryConsPredicate(tabName, columnName, FOperator.EQ, cons, innerTabCols);

                    // order matters!!
                    predicateIndexer.put(t0);
                    predicateIndexer.put(t1);
                    predicateIndexer.put(t01);

                    predicateIndexer.insertConstPredT01Map(t0, t1);
                    predicateIndexer.insertConstBiPred2DivisionPred(t01, t0, t1);

                });
                columnInfo.getIntervalConstants().forEach(interval -> {
                    FIPredicate t0 = new FUnaryIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols);
                    FIPredicate t1 = new FUnaryIntervalConsPredicate(tabName, columnName, 1, interval, innerTabCols);
                    FIPredicate t01 = new FBinaryIntervalConsPredicate(tabName, columnName, interval, innerTabCols);

                    predicateIndexer.put(t0);
                    predicateIndexer.put(t1);
                    predicateIndexer.put(t01);

                    predicateIndexer.insertConstPredT01Map(t0, t1);
                    predicateIndexer.insertConstBiPred2DivisionPred(t01, t0, t1);
                });
            }

            if (columnInfo.getColumnType().equals(FColumnType.NORMAL)) {
                predicateIndexer.put(new FBinaryPredicate(tabName, tabName, columnName, columnName, FOperator.EQ, innerTabCols));
            }

            if (columnInfo.getColumnType().equals(FColumnType.EXTERNAL_BINARY_MODEL)) {
                predicateIndexer.put(new FBinaryModelPredicate(tabName, tabName, columnName, innerTabCols));
            }

        });

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);


        return predicateIndexer;
    }

    public static synchronized FPredicateIndexer createMultiTableCrossLinePredicates(
            FTableInfo leftTable, FTableInfo rightTable,
            FDerivedColumnNameHandler derivedColumnNameHandler, FPLI PLI, FTableDatasetMap datasetMap,
            double crossColumnThreshold, List<FExternalPredicateInfo> otherInfos) {
        FREQUENT_TABLE_CACHE.clear();
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
                if (columnSimilarity(leftTable, leftColumnInfo, rightTable, rightColumnInfo, PLI, datasetMap) >= crossColumnThreshold) {
                    predicateIndexer.put(new FBinaryPredicate(leftTableName, rightTableName, leftColumnName, rightColumnName, FOperator.EQ, innerTabCols.get()));
                }

            }); // end right loop
        }); // end left loop

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        FREQUENT_TABLE_CACHE.clear();

        return predicateIndexer;
    }

    private static Map<FPair<FTableInfo, FColumnName>, JavaPairRDD<Long, Long>> FREQUENT_TABLE_CACHE = new HashMap<>();

    private static double columnSimilarity(final FTableInfo leftTable, final FColumnInfo leftColumnInfo,
                                           final FTableInfo rightTable, final FColumnInfo rightColumnInfo,
                                           final FPLI PLI, final FTableDatasetMap datasetMap) {
        final FColumnName leftColumnName = new FColumnName(leftColumnInfo.getColumnName());
        final FColumnName rightColumnName = new FColumnName(rightColumnInfo.getColumnName());

        final JavaPairRDD<Long, Long> leftFrequentTable = FREQUENT_TABLE_CACHE.computeIfAbsent(
                new FPair<>(leftTable, leftColumnName), key -> findFrequentTable(key, PLI));

        final JavaPairRDD<Long, Long> rightFrequentTable = FREQUENT_TABLE_CACHE.computeIfAbsent(
                new FPair<>(rightTable, rightColumnName), key -> findFrequentTable(key, PLI));

        if (leftFrequentTable.count() == 0L || rightFrequentTable.count() == 0L) return 0D;

        final double shareNumber = leftFrequentTable.join(rightFrequentTable)
                .map(Tuple2::_2).map(ff -> Math.min(ff._1, ff._2))
                .fold(0L, Long::sum).doubleValue();
        if (shareNumber == 0L) return 0D;

        final long leftLength = leftTable.getLength(() -> datasetMap.getDatasetByTableName(leftTable.getTableName()).count());
        final long rightLength = rightTable.getLength(() -> datasetMap.getDatasetByTableName(rightTable.getTableName()).count());

        return Math.max(shareNumber / leftLength, shareNumber / rightLength);

    }

    private static JavaPairRDD<Long, Long> findFrequentTable(FPair<FTableInfo, FColumnName> tabCol, FPLI PLI) {
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


}
