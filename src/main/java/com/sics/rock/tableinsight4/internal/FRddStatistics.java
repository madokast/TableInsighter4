package com.sics.rock.tableinsight4.internal;

import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.SizeEstimator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * statistics of any rdd
 *
 * @author zhaorx
 */
public class FRddStatistics {

    /**
     * sample size for statistics
     */
    private static final int SAMPLE_SIZE = 256;

    /**
     * the rdd size in byte if read into memory completely
     */
    private final long estimatedSize;

    /**
     * rdd.count()
     */
    private final long count;

    /**
     * rdd.numPartitions
     */
    private final int partitionNumber;

    /**
     * element size in each partition
     */
    private final Map<Integer, Long> partitionCount;


    private FRddStatistics(Map<Integer, Long> partitionCount, List<?> sampleData) {
        this.count = partitionCount.values().stream().mapToLong(Long::longValue).sum();
        this.partitionNumber = partitionCount.size();
        this.partitionCount = partitionCount;

        // trimToSize
        sampleData = new ArrayList<>(sampleData);

        final double size = SizeEstimator.estimate(sampleData) * 1D / sampleData.size() * this.count;

        this.estimatedSize = (long) size;
    }

    public static FRddStatistics apply(JavaPairRDD<?, ?> rdd) {

        Map<Integer, Long> partitionCount = rdd.mapPartitions(iter -> {
            final int partitionId = TaskContext.get().partitionId();
            long count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            return Collections.singletonList(new FPair<>(partitionId, count)).iterator();
        }).collect().stream().collect(Collectors.toMap(FPair::k, FPair::v));

        List<?> sampleData = rdd.takeSample(true, SAMPLE_SIZE);

        FAssertUtils.require(() -> partitionCount.size() == rdd.getNumPartitions(),
                () -> "Bad statistics! info " + sampleData.size() + " " + rdd.getNumPartitions());
        FAssertUtils.require(() -> partitionCount.values().stream().mapToLong(Long::longValue).sum() == rdd.count(),
                () -> "Bad statistics! info " + partitionCount.values().stream().mapToLong(Long::longValue).sum() + " " + rdd.count());

        return new FRddStatistics(partitionCount, sampleData);
    }

    public static FRddStatistics apply(JavaRDD<?> rdd) {

        Map<Integer, Long> partitionCount = rdd.mapPartitions(iter -> {
            final int partitionId = TaskContext.get().partitionId();
            long count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            return Collections.singletonList(new FPair<>(partitionId, count)).iterator();
        }).collect().stream().collect(Collectors.toMap(FPair::k, FPair::v));

        List<?> sampleData = rdd.takeSample(true, SAMPLE_SIZE);

        FAssertUtils.require(() -> partitionCount.size() == rdd.getNumPartitions(),
                () -> "Bad statistics! info " + sampleData.size() + " " + rdd.getNumPartitions());
        FAssertUtils.require(() -> partitionCount.values().stream().mapToLong(Long::longValue).sum() == rdd.count(),
                () -> "Bad statistics! info " + partitionCount.values().stream().mapToLong(Long::longValue).sum() + " " + rdd.count());

        return new FRddStatistics(partitionCount, sampleData);
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    public double getEstimatedSizeMB() {
        return estimatedSize / 1024D / 1024D;
    }

    public String getEstimatedSizeMBString() {
        return String.format("%.3f", getEstimatedSizeMB());
    }

    public long getCount() {
        return count;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public Map<Integer, Long> getPartitionCount() {
        return partitionCount;
    }

}
