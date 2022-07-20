package com.sics.rock.tableinsight4.internal;

import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FRddElementIndexUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.List;

public class FRddElementIndexTest extends FSparkEnv {

    @Test
    public void test() {
        JavaRDD<Integer> rdd = randRDD(() -> random.nextInt(), 100).repartition(3);
        JavaRDD<String> infoRdd = FRddElementIndexUtils.rddElementIndex(rdd).map(Tuple2::_2).map(FRddElementIndex::toString);

        infoRdd.map(s -> "info " + s).foreach(logger::info);

        FRddElementIndex i04 = new FRddElementIndex(0, 4);
        FRddElementIndex i15 = new FRddElementIndex(1, 5);
        FRddElementIndex i22 = new FRddElementIndex(2, 2);

        List<FRddElementIndex> is = FTiUtils.listOf(i15, i04, i22);

        FRddElementIndexUtils.filterElementsByIndices(infoRdd, is)
                .stream().map(s -> "find" + s).forEach(logger::info);
    }

}