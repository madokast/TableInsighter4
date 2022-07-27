package com.sics.rock.tableinsight4.pli;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.procedure.interval.FInterval;
import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Local inverted index table.
 * Map[Long, List[Integer]]
 * Key is index
 * Value is local rowId in partition
 * <p>
 * index -1 fro null value
 */
public class FLocalPLI implements Serializable {

    // index -> [local rowId]
    private final Map<Long, List<Integer>> index2localRowIds = new HashMap<>();


    private int maxLocalRowId = -1;

    /**
     * A partition create a FLocalPLI
     * For debug and assert
     */
    private final int partitionId;

    private FLocalPLI(int partitionId) {
        this.partitionId = partitionId;
    }

    public static FLocalPLI singleLinePLI(int partitionId, long index, int localRowId) {
        FLocalPLI pli = new FLocalPLI(partitionId);
        pli.put(index, localRowId);
        return pli;
    }

    public FLocalPLI merge(FLocalPLI others) {
        FAssertUtils.require(() -> this.partitionId == others.partitionId,
                () -> "Merge two FLocalPLI from different partitions. The partitionIds are "
                        + this.partitionId + " and " + others.partitionId);
        others.index2localRowIds.forEach(this::put);
        return this;
    }

    public Stream<Integer> localRowIdsOf(long index, FOperator operator) {
        switch (operator) {
            case EQ:
                return index2localRowIds.getOrDefault(index, Collections.emptyList()).stream();
            case GT:
                return index2localRowIds.entrySet().stream().filter(e -> e.getKey() > index).map(Map.Entry::getValue).flatMap(List::stream);
            case LT:
                return index2localRowIds.entrySet().stream().filter(e -> e.getKey() < index && e.getKey() >= 0).map(Map.Entry::getValue).flatMap(List::stream);
            case GET:
                return index2localRowIds.entrySet().stream().filter(e -> e.getKey() >= index).map(Map.Entry::getValue).flatMap(List::stream);
            case LET:
                return index2localRowIds.entrySet().stream().filter(e -> e.getKey() <= index && e.getKey() >= 0).map(Map.Entry::getValue).flatMap(List::stream);
            default:
                throw new RuntimeException("Operator " + operator + " not support");
        }
    }

    public Stream<Integer> localRowIdsBetween(long leftIndex, long rightIndex, boolean leftClose, boolean rightClose) {
        if (leftClose && rightClose) {
            return index2localRowIds.entrySet().stream()
                    .filter(e -> {
                        long key = e.getKey();
                        return key >= leftIndex && key <= rightIndex;
                    })
                    .map(Map.Entry::getValue).flatMap(List::stream);
        } else if (leftClose) { // leftClose && !rightClose
            return index2localRowIds.entrySet().stream()
                    .filter(e -> {
                        long key = e.getKey();
                        return key >= leftIndex && key < rightIndex;
                    })
                    .map(Map.Entry::getValue).flatMap(List::stream);
        } else if (rightClose) { // !leftClose && rightClose
            return index2localRowIds.entrySet().stream()
                    .filter(e -> {
                        long key = e.getKey();
                        return key > leftIndex && key <= rightIndex;
                    })
                    .map(Map.Entry::getValue).flatMap(List::stream);
        } else {
            return index2localRowIds.entrySet().stream()
                    .filter(e -> {
                        long key = e.getKey();
                        return key > leftIndex && key < rightIndex;
                    })
                    .map(Map.Entry::getValue).flatMap(List::stream);
        }
    }

    public Stream<Long> indexStream() {
        return index2localRowIds.keySet().stream();
    }

    private void put(Long index, Integer localRowId) {
        final List<Integer> rowIds = index2localRowIds.getOrDefault(index, new ArrayList<>());
        rowIds.add(localRowId);
        index2localRowIds.put(index, rowIds);
        maxLocalRowId = Math.max(maxLocalRowId, localRowId);
    }

    private void put(Long index, List<Integer> localRowIds) {
        if (localRowIds.isEmpty()) return;

        put(index, localRowIds.get(0));
        final List<Integer> targetList = index2localRowIds.get(index);

        for (int i = 1; i < localRowIds.size(); i++) {
            final Integer localRowId = localRowIds.get(i);
            targetList.add(localRowId);
            maxLocalRowId = Math.max(maxLocalRowId, localRowId);
        }
    }

    public int getMaxLocalRowId() {
        return maxLocalRowId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public Map<Long, List<Integer>> getIndex2localRowIds() {
        return index2localRowIds;
    }

    @Override
    public String toString() {
        return "pid(" + partitionId + ")" +
                index2localRowIds.entrySet().stream().map(e -> "[" + e.getKey() + ":" +
                        e.getValue().stream().map(Objects::toString).collect(Collectors.joining(",")) + "]")
                        .collect(Collectors.joining("", "", ""));
    }

}
