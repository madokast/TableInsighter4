package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;

public class FKMeansUtilsTest extends FSparkEnv {

    @Test
    public void findInterval() {
        final JavaRDD<Double> doubleJavaRDD = sc.parallelize(FUtils.listOf(1., 1., 1., 2., 2., 2., 3., 3., 3.));

        final List<FPair<Double, Double>> intervals = FKMeansUtils.findIntervals(doubleJavaRDD, 3, 20);

        for (FPair<Double, Double> interval : intervals) {
            logger.info("interval " + interval);
        }
    }
}