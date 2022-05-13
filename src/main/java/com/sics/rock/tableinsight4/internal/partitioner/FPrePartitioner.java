package com.sics.rock.tableinsight4.internal.partitioner;

import org.apache.spark.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pre assignment Partitioner
 * Using when we know all the keys of a rdd
 * and want each key occupy one partitioner
 *
 * @author zhaorx
 */
public class FPrePartitioner extends Partitioner {

    private static final Logger logger = LoggerFactory.getLogger(FPrePartitioner.class);

    private final Map<Object, Integer> key2pid;

    private final int numPartitions;

    /**
     * @param keys all keys of the rdd
     */
    public FPrePartitioner(List<?> keys) {
        this.key2pid = new HashMap<>();
        this.numPartitions = keys.size();

        for (int i = 0; i < keys.size(); i++) {
            this.key2pid.put(keys.get(i), i);
        }

        if (key2pid.size() != numPartitions) {
            throw new RuntimeException("There are duplicates in keys " + keys);
        }
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return key2pid.get(key);
    }
}
