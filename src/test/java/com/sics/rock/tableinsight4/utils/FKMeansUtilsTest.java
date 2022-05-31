package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.test.FSparkTestEnv;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FKMeansUtilsTest extends FSparkTestEnv {

    @Test
    public void findRanges() {
        final JavaRDD<Double> doubleJavaRDD = sc.parallelize(FUtils.listOf(1., 1., 1., 2., 2., 2., 3., 3., 3.));

        final List<FPair<Double, Double>> ranges = FKMeansUtils.findRanges(doubleJavaRDD, 3, 20);

        for (FPair<Double, Double> range : ranges) {
            logger.info("Range " + range);
        }
    }
}