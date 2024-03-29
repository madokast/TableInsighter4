package com.sics.rock.tableinsight4.internal.partitioner;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;


public class FPrePartitionerTest extends FSparkEnv {

    private static final Logger logger = LoggerFactory.getLogger(FPrePartitionerTest.class);

    @Test
    public void test() {
        final List<Double> keys = IntStream.range(0, 100).mapToObj(i -> Math.random())
                .distinct().collect(Collectors.toList());

        final FPrePartitioner prePartitioner = new FPrePartitioner(keys);

        final JavaPairRDD<Double, Double> pairRdd = sc.parallelize(keys).mapToPair(k -> new Tuple2<>(k, k + k));

        final JavaPairRDD<Double, Double> repartitionOne = pairRdd.repartition(1);

        final List<Integer> partitionNumberOne = repartitionOne
                .mapPartitions(iter -> Collections.singletonList(TaskContext.get().partitionId()).iterator())
                .distinct().collect();

        assertEquals(1, partitionNumberOne.size());

        repartitionOne.partitionBy(prePartitioner).foreachPartition(iter -> {
            final List<Tuple2<Double, Double>> list = FTiUtils.collect(iter);
            assertEquals(1, list.size());
            final Double key = list.get(0)._1;
            final int prePartitionId = prePartitioner.getPartition(key);
            final int curPid = TaskContext.get().partitionId();
            logger.debug("key {} prePid {} curPid {}", key, prePartitionId, curPid);
            assertEquals(prePartitionId, curPid);
        });
    }

}