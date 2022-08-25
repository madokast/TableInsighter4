package com.sics.rock.tableinsight4.internal;

import org.apache.spark.api.java.JavaPairRDD;

public class FLocalPairRDDPartitionIterable<K, V> extends FLocalBatchPairRDDIterable<K, V> {

    private FLocalPairRDDPartitionIterable(JavaPairRDD<K, V> RDD) {
        super(RDD, 1);
    }

    public static <K, V> FLocalPairRDDPartitionIterable<K, V> of(JavaPairRDD<K, V> RDD) {
        return new FLocalPairRDDPartitionIterable<>(RDD);
    }
}
