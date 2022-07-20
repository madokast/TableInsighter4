package com.sics.rock.tableinsight4.internal.partitioner;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FSparkUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

import static org.junit.Assert.*;

public class FOrderedPartitionerTest extends FSparkEnv {

    @Test
    public void test() {
        List<String> strings = FTiUtils.listOf("a", "b", "c", "d", "e");

        JavaRDD<String> rdd = sc.parallelize(strings, 2);

        JavaPairRDD<Long, String> orderedRDD = FSparkUtils.addOrderedId(rdd);

        logger.info("{} - {}", orderedRDD.getNumPartitions(), orderedRDD.collect());

        FOrderedPartitioner orderedPartitioner = new FOrderedPartitioner(orderedRDD.count(), 2);

        JavaPairRDD<Long, String> orderedRDD2 = orderedRDD.partitionBy(orderedPartitioner);

        logger.info("{} - {}", orderedRDD2.getNumPartitions(), orderedRDD2.collect());

        orderedRDD2.foreachPartition(iter -> {
            final int pid = TaskContext.getPartitionId();
            while (iter.hasNext()) {
                final Tuple2<Long, String> t = iter.next();
                assertEquals(pid, t._1 / 2);
            }
        });
    }

}