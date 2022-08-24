package com.sics.rock.tableinsight4.internal;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A batch iterable.
 * The element is list consisting of tuple2
 * <p>
 * The class is used for iteration of a pair-rdd in local and batch
 * <p>
 * note: suppose the data is uniform distributed in RDD
 */
public class FLocalBatchPairRDDIterable<K, V> implements Iterable<List<Tuple2<K, V>>>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FLocalBatchPairRDDIterable.class);

    private transient final JavaPairRDD<K, V> RDD;

    /**
     * the number of partitions in each batch
     */
    private transient final int batchPartitionNumber;

    private transient final List<Tuple2<K, V>> firstBatch;


    private FLocalBatchPairRDDIterable(JavaPairRDD<K, V> RDD, int batchPartitionNumber) {
        this.RDD = RDD;
        this.batchPartitionNumber = batchPartitionNumber;
        this.firstBatch = filterCollect(RDD, 0, batchPartitionNumber);
    }

    private static <K, V> List<Tuple2<K, V>> filterCollect(JavaPairRDD<K, V> RDD, int startingPartitionId, int endingPartitionId) {
        return RDD.mapPartitions(iter -> {
            int pid = TaskContext.getPartitionId();
            if (pid >= startingPartitionId && pid < endingPartitionId) return iter;
            else return Collections.emptyIterator();
        }).collect();
    }

    public static <K, V> FLocalBatchPairRDDIterable<K, V> of(JavaPairRDD<K, V> RDD, long batchSizeMB) {
        final long batchSizeByte = batchSizeMB * 1024 * 1024;
        final FRddStatistics statistics = FRddStatistics.apply(RDD);
        final long partitionNumber = statistics.getPartitionNumber();
        final long totalSizeByte = statistics.getEstimatedSize();
        double eachBathSizeByte = totalSizeByte * 1D / partitionNumber;
        final int batchPartitionNumber = ((int) (batchSizeByte / eachBathSizeByte)) + 1;
        logger.info("Create LocalBatchPairRDDIterable. RDD size is {} MB and partitionNumber is {}. " +
                        "Each batch contains {} partition(s). A batch size is about {} MB",
                statistics.getEstimatedSizeMB(), partitionNumber, batchPartitionNumber,
                eachBathSizeByte * batchPartitionNumber / 1024 / 1024);
        return new FLocalBatchPairRDDIterable<>(RDD, batchPartitionNumber);
    }

    @Override
    public Iterator<List<Tuple2<K, V>>> iterator() {
        return this.new FLocalBatchPairRDDIterator();
    }

    class FLocalBatchPairRDDIterator implements Iterator<List<Tuple2<K, V>>>, Serializable {

        private transient int current = FLocalBatchPairRDDIterable.this.batchPartitionNumber;

        private transient List<Tuple2<K, V>> next = FLocalBatchPairRDDIterable.this.firstBatch;

        @Override
        public boolean hasNext() {
            return !next.isEmpty();
        }

        @Override
        public List<Tuple2<K, V>> next() {
            final List<Tuple2<K, V>> theNext = this.next;
            this.next = FLocalBatchPairRDDIterable.filterCollect(
                    FLocalBatchPairRDDIterable.this.RDD, current,
                    current + FLocalBatchPairRDDIterable.this.batchPartitionNumber
            );
            this.current += FLocalBatchPairRDDIterable.this.batchPartitionNumber;
            return theNext;
        }
    }
}
