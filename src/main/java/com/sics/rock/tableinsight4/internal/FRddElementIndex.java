package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;

/**
 * The position of an element in rdd is determined by its partitionId and offset.
 *
 * @author zhaorx
 */
public class FRddElementIndex implements Serializable {

    public final int partitionId;

    public final int offset;

    public FRddElementIndex(int partitionId, int offset) {
        this.partitionId = partitionId;
        this.offset = offset;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FRddElementIndex that = (FRddElementIndex) o;
        return partitionId == that.partitionId &&
                offset == that.offset;
    }

    @Override
    public int hashCode() {
        return (partitionId << 16) + offset;
    }

    @Override
    public String toString() {
        return partitionId + ":" + offset;
    }
}
