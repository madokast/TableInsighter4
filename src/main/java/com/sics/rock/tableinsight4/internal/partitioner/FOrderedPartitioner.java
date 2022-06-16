package com.sics.rock.tableinsight4.internal.partitioner;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.Partitioner;

import java.util.Objects;

/**
 * A partitioner used for pair-rdd[Long, Any] only.
 * The capacity of each partition is C.
 * A element (k:Long, v:Any) in the rdd will be distributed to (k/C)-th partition.
 * <p>
 * The key value must be greater then or equal 0
 * <p>
 * Since the number of partitions should be determined before shuffle,
 * the max key is required to calculate the C.
 * <p>
 * In practical, the rdd is constructed by FSparkUtils.orderedIndex(),
 * which works like zipWithIndex but keeps the index form 0-count consecutively.
 * For example, the pair-rdd is
 * [(0,a), (2,c), (4,e)], [(1,b), (3,d), (5,f)]
 * and use FOrderedPartitioner(elementSize=rdd.count, partitionCapacity=2).
 * <p>
 * After shuffle, the rdd obtained is
 * [(0,a), (1,b)] partition-id = 0
 * [(2,c), (3,d)] partition-id = 1
 * [(4,e), (5,f)] partition-id = 2
 * <p>
 * In table-insight, the partitioner is used for the construction of FLocalPLI
 *
 * @author zhaorx
 */
public class FOrderedPartitioner extends Partitioner {

    /**
     * Capacity of each partition
     */
    private final int partitionCapacity;

    /**
     * Number of partitions
     */
    private final int numPartitions;

    /**
     * If the rdd is constructed by FSparkUtils.orderedIndex(),
     * elementSize is just the rdd.count
     * <p>
     * If keys of the rdd just keep greater then or equal 0,
     * the elementSize should be max(keys)+1
     *
     * @param elementSize       total element size of rdd.
     * @param partitionCapacity capacity of each partition
     */
    public FOrderedPartitioner(long elementSize, int partitionCapacity) {
        this.partitionCapacity = partitionCapacity;
        long numPart = elementSize / partitionCapacity;
        if (elementSize % partitionCapacity != 0) ++numPart;
        if (numPart > Integer.MAX_VALUE) throw new RuntimeException("Cannot create RangePartitioner，the elementSize "
                + elementSize + " and  partitionCapacity " + partitionCapacity + " result the numPartitions " + numPart + " exceeding INT_MAX");
        this.numPartitions = (int) numPart;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        FAssertUtils.require(() -> key != null, () -> "Key of the rdd and RangePartitioner should not be null");
        FAssertUtils.require(() -> key instanceof Long, () -> "Key of the rdd and RangePartitioner should be Long type, not " + key.getClass());
        return (int) (((long) key) / partitionCapacity);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FOrderedPartitioner that = (FOrderedPartitioner) o;
        return partitionCapacity == that.partitionCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitionCapacity);
    }
}
