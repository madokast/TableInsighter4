package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;

/**
 * typedef Integer FPartitionId
 * for readability only
 *
 * @author zhaorx
 */
public class FPartitionId implements Serializable {

    public final int value;

    private FPartitionId(int value) {
        this.value = value;
    }

    public static FPartitionId of(int value) {
        return new FPartitionId(value);
    }

    @Override
    public String toString() {
        return Integer.toString(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FPartitionId that = (FPartitionId) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return value;
    }
}
