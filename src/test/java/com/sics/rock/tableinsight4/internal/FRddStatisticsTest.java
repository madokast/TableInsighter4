package com.sics.rock.tableinsight4.internal;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.SizeEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FRddStatisticsTest extends FSparkEnv {

    @Test
    public void test() {
        List<List<Integer>> collect = IntStream.range(0, 30).mapToObj(_i -> {
            int num = random.nextInt(100) + 1;
            return IntStream.range(0, num).map(_j -> random.nextInt()).boxed().collect(Collectors.toList());
        }).collect(Collectors.toList());

        JavaRDD<Integer> rdd = sc.parallelize(collect, collect.size())
                .mapPartitions(iter -> iter.next().iterator(), true);

        FRddStatistics statistics = FRddStatistics.apply(rdd);

        Assert.assertEquals(collect.size(), statistics.getPartitionNumber());
        Assert.assertEquals(rdd.count(), statistics.getCount());

        logger.info("rdd size " + statistics.getEstimatedSize());
        logger.info("rdd size mb " + statistics.getEstimatedSizeMB());
        logger.info("rdd true size " + SizeEstimator.estimate(rdd.collect()));
    }
}