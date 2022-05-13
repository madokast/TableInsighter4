package com.sics.rock.tableinsight4.internal.partitioner;

import com.sics.rock.tableinsight4.HadoopEnv4Junit;
import com.sics.rock.tableinsight4.utils.FSparkUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

@RunWith(HadoopEnv4Junit.class)
public class FPrePartitionerTest {

    private static final Logger logger = LoggerFactory.getLogger(FPrePartitionerTest.class);

    @Test
    public void test() {
        final List<Double> keys = IntStream.range(0, 100).mapToObj(i -> Math.random())
                .distinct().collect(Collectors.toList());

        final FPrePartitioner prePartitioner = new FPrePartitioner(keys);

        final SparkSession sparkSession = FSparkUtils.localSparkSession();

        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        final JavaPairRDD<Double, Double> pairRdd = sc.parallelize(keys).mapToPair(k -> new Tuple2<>(k, k + k));

        final JavaPairRDD<Double, Double> repartitionOne = pairRdd.repartition(1);

        final List<Integer> partitionNumberOne = repartitionOne
                .mapPartitions(iter -> Collections.singletonList(TaskContext.get().partitionId()).iterator())
                .distinct().collect();

        assertEquals(1, partitionNumberOne.size());

        repartitionOne.partitionBy(prePartitioner).foreachPartition(iter -> {
            final List<Tuple2<Double, Double>> list = FUtils.collect(iter);
            assertEquals(1, list.size());
            final Double key = list.get(0)._1;
            final int prePartitionId = prePartitioner.getPartition(key);
            final int curPid = TaskContext.get().partitionId();
            logger.info("key {} prePid {} curPid {}", key, prePartitionId, curPid);
            assertEquals(prePartitionId, curPid);
        });
    }

}