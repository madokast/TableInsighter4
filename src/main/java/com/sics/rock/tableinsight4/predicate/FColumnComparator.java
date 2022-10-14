package com.sics.rock.tableinsight4.predicate;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.FSerializableComparator;
import com.sics.rock.tableinsight4.pli.FLocalPLI;
import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FColumnComparator {

    private final FPLI PLI;

    private final FTableDatasetMap datasetMap;

    private final FTableInfo leftTable;

    private final FTableInfo rightTable;

    private Map<FPair<FTableInfo, FColumnName>, JavaPairRDD<Long, Long>> frequentColumnCache = new HashMap<>();

    public boolean columnComparable(final FColumnInfo leftColumnInfo, final FColumnInfo rightColumnInfo) {
        final FColumnName leftColumnName = new FColumnName(leftColumnInfo.getColumnName());
        final FColumnName rightColumnName = new FColumnName(rightColumnInfo.getColumnName());

        final JavaPairRDD<Long /*Index*/, Long /*Count*/> leftFrequentTable = frequentColumnCache.computeIfAbsent(
                new FPair<>(leftTable, leftColumnName), this::findFrequentTable);

        final JavaPairRDD<Long /*Index*/, Long /*Count*/> rightFrequentTable = frequentColumnCache.computeIfAbsent(
                new FPair<>(rightTable, rightColumnName), this::findFrequentTable);

        final JavaRDD<Long /*Index*/> leftIndexRDD = leftFrequentTable.map(Tuple2::_1).filter(index -> index >= 0).cache();
        final JavaRDD<Long /*Index*/> rightIndexRDD = rightFrequentTable.map(Tuple2::_1).filter(index -> index >= 0).cache();

        if (leftIndexRDD.count() == 0L || rightIndexRDD.count() == 0L) return false;


        final long leftMaxIndex = leftIndexRDD.max(FSerializableComparator.LONG_COMPARATOR);
        final long leftMinIndex = leftIndexRDD.min(FSerializableComparator.LONG_COMPARATOR);

        final long rightMaxIndex = rightIndexRDD.max(FSerializableComparator.LONG_COMPARATOR);
        final long rightMinIndex = rightIndexRDD.min(FSerializableComparator.LONG_COMPARATOR);

        // rightMaxIndex in [leftMinIndex, leftMaxIndex] or rightMinIndex in [leftMinIndex, leftMaxIndex]
        return (rightMaxIndex <= leftMaxIndex && rightMaxIndex >= leftMinIndex) ||
                (rightMinIndex <= leftMaxIndex && rightMinIndex >= leftMinIndex);
    }


    public double columnSimilarity(final FColumnInfo leftColumnInfo,
                                    final FColumnInfo rightColumnInfo) {
        final FColumnName leftColumnName = new FColumnName(leftColumnInfo.getColumnName());
        final FColumnName rightColumnName = new FColumnName(rightColumnInfo.getColumnName());

        final JavaPairRDD<Long /*Index*/, Long /*Count*/> leftFrequentTable = frequentColumnCache.computeIfAbsent(
                new FPair<>(leftTable, leftColumnName), this::findFrequentTable);

        final JavaPairRDD<Long  /*Index*/, Long /*Count*/> rightFrequentTable = frequentColumnCache.computeIfAbsent(
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

    public FColumnComparator(final FPLI PLI, final FTableDatasetMap datasetMap,
                             final FTableInfo leftTable, final FTableInfo rightTable) {
        this.PLI = PLI;
        this.datasetMap = datasetMap;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
    }
}
