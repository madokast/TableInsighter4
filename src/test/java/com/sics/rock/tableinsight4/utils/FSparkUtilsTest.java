package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.test.FSparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class FSparkUtilsTest extends FSparkEnv {

    @Test
    public void swapKey() {
        // a -> b -> c
        JavaPairRDD<String, String> bc = sc.parallelize(Collections.singletonList(new FPair<>("b", "c")))
                .mapToPair(p -> new Tuple2<>(p._k, p._v));
        Map<String, JavaPairRDD<String, String>> abc = Collections.singletonMap("a", bc);
        List<Tuple2<String, Map<String, String>>> bac = FSparkUtils.swapKey(abc).collect();

        assertEquals(1, bac.size());
        Tuple2<String, Map<String, String>> bacTuple = bac.get(0);

        assertEquals("b", bacTuple._1);

        assertEquals(Collections.singletonMap("a", "c"), bacTuple._2);
    }

    @Test
    public void swapKey2() {
        Map<Integer, JavaPairRDD<Integer, Integer>> kkv = IntStream.range(0, 100).mapToObj(k1 -> {
            int k2 = k1 * 2;
            int val = k1 + k1;
            JavaPairRDD<Integer, Integer> k2val = sc.parallelize(Collections.singletonList(new FPair<>(k2, val)))
                    .mapToPair(p -> new Tuple2<>(p._k, p._v));
            return new FPair<>(k1, k2val);
        }).collect(Collectors.toMap(FPair::k, FPair::v));

        List<Tuple2<Integer, Map<Integer, Integer>>> swapped = FSparkUtils.swapKey(kkv).collect();

        assertEquals(kkv.size(), swapped.size());

        swapped.forEach(t -> {
            int k2 = t._1;
            Map<Integer, Integer> k1v = t._2;
            assertEquals(1, k1v.size());
            Map.Entry<Integer, Integer> k1vEntry = k1v.entrySet().iterator().next();
            int k1 = k1vEntry.getKey();
            int val = k1vEntry.getValue();

            assertEquals(k2, k1 * 2);
            assertEquals(val, k1 + k1);
        });
    }

    @Test
    public void swapKey3() {
        Map<Integer, JavaPairRDD<Integer, Integer>> kkv = Stream
                .generate(random::nextInt)
                .limit(100)
                .distinct()
                .map(k1 -> {
                    int k2 = k1 * 2;
                    int val = k1 + k1;
                    JavaPairRDD<Integer, Integer> k2val = sc.parallelize(Collections.singletonList(new FPair<>(k2, val)))
                            .mapToPair(p -> new Tuple2<>(p._k, p._v));
                    return new FPair<>(k1, k2val);
                }).collect(Collectors.toMap(FPair::k, FPair::v));

        List<Tuple2<Integer, Map<Integer, Integer>>> swapped = FSparkUtils.swapKey(kkv).collect();

        assertEquals(kkv.size(), swapped.size());

        swapped.forEach(t -> {
            int k2 = t._1;
            Map<Integer, Integer> k1v = t._2;
            assertEquals(1, k1v.size());
            Map.Entry<Integer, Integer> k1vEntry = k1v.entrySet().iterator().next();
            int k1 = k1vEntry.getKey();
            int val = k1vEntry.getValue();

            assertEquals(k2, k1 * 2);
            assertEquals(val, k1 + k1);
        });
    }

    @Test
    public void orderedIndex() {
    }

    @Test
    public void addOrderedId() {
    }

    @Test
    public void unionReduceLong() {
    }

    @Test
    public void union() {
    }
}