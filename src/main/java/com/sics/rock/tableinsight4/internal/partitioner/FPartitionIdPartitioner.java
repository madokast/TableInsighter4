package com.sics.rock.tableinsight4.internal.partitioner;

import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.Partitioner;

/**
 * The FPartitionIdPartitioner worked on pair-rdd which the key is partitionId
 * <p>
 * rdd.mapToPair(e->(pid, e)).shuffle.reduce(new FPartitionIdPartitioner, func)
 *
 * @author zhaorx
 */
public class FPartitionIdPartitioner extends Partitioner {

    private final int numPartitions;

    public FPartitionIdPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        FAssertUtils.require(() -> key != null,
                () -> "Pair-rdd key is Null in FPartitionIdPartitioner");
        FAssertUtils.require(() -> key instanceof FPartitionId,
                () -> "Pair-rdd key type " + key.getClass() + " is not Integer in FPartitionIdPartitioner");
        FAssertUtils.require(() -> ((FPartitionId) key).value < numPartitions,
                () -> "Pair-rdd key " + key + " should less then numPartitions "
                        + numPartitions + " for recognition as partitionId in FPartitionIdPartitioner");

        return ((FPartitionId) key).value;
    }
}
