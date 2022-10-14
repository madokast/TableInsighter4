package com.sics.rock.tableinsight4.others;

import com.sics.rock.tableinsight4.internal.FSerializableComparator;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.io.Serializable;
import java.util.Comparator;

public class FLongRDDMaxTest extends FSparkEnv {

    @Test
    public void test() {
        final JavaRDD<Long> longRDD = sc.parallelize(FTiUtils.listOf(1L, 2L, 3L, 4L));


        // Task not serializable
        // final Long max = longRDD.max(Long::compareTo);
        // logger.info("max = " + max);


        final Long max = longRDD.max(new LongComparator());
        logger.info("max = " + max);

        final Long max2 = longRDD.max(FSerializableComparator.LONG_COMPARATOR);
        logger.info("max2 = " + max);
    }

    public static class LongComparator implements Serializable, Comparator<Long> {
        @Override
        public int compare(final Long o1, final Long o2) {
            return Long.compare(o1, o2);
        }
    }

}
