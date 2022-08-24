package com.sics.rock.tableinsight4.internal;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class FLocalBatchPairRDDIterableTest extends FSparkEnv {

    @Test
    public void test() {
        final JavaPairRDD<Integer, Object> rdd = spark.range(0, 1000, 1, 100).toJavaRDD()
                .mapToPair(l -> new Tuple2<>(l.intValue(), new int[1024]));

        for (List<Tuple2<Integer, Object>> batch : FLocalBatchPairRDDIterable.of(rdd, 1)) {
            logger.info("batch size = {} first ele {} last {}", batch.size(), batch.get(0), batch.get(batch.size() - 1));
        }
    }

}