package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Tuple2;

import java.util.*;
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
        Map<Integer, JavaPairRDD<Integer, Integer>> kkv = IntStream.range(0, 35).mapToObj(k1 -> {
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
                .limit(30)
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
        for (int size = 20; size < 1000; size += 100) {
            List<Integer> data = Stream.generate(random::nextInt).limit(size).collect(Collectors.toList());
            JavaRDD<Integer> rdd = sc.parallelize(data);
            JavaPairRDD<Integer, Long> rddIndex = FSparkUtils.orderedIndex(rdd);
            rddIndex.foreach(t -> logger.debug("{}", t));
            List<Long> idCollect = new ArrayList<>(rddIndex.map(Tuple2::_2).distinct().collect());
            int idSize = idCollect.size();
            assertEquals(size, idSize);

            idCollect.sort(Comparator.comparingLong(Long::longValue));
            for (int i = 0; i < idCollect.size(); i++) {
                assertEquals(i, idCollect.get(i).longValue());
            }
        }
    }

    @Test
    public void orderedIndex2() {
        for (int pNum = 1; pNum < 50; pNum += 10) {
            int size = 100;
            List<Integer> data = Stream.generate(random::nextInt).limit(size).collect(Collectors.toList());
            JavaRDD<Integer> rdd = sc.parallelize(data).repartition(pNum);
            JavaPairRDD<Integer, Long> rddIndex = FSparkUtils.orderedIndex(rdd);
            assertEquals(pNum, rddIndex.getNumPartitions());
            rddIndex.foreach(t -> logger.debug("{}", t));
            List<Long> idCollect = new ArrayList<>(rddIndex.map(Tuple2::_2).distinct().collect());
            int idSize = idCollect.size();
            assertEquals(size, idSize);

            idCollect.sort(Comparator.comparingLong(Long::longValue));
            for (int i = 0; i < idCollect.size(); i++) {
                assertEquals(i, idCollect.get(i).longValue());
            }
        }
    }

    @Test
    public void addOrderedId() {
        for (int size = 20; size < 1000; size += 100) {
            List<Integer> data = Stream.generate(random::nextInt).limit(size).collect(Collectors.toList());
            JavaRDD<Integer> rdd = sc.parallelize(data);
            JavaPairRDD<Long, Integer> rddIndex = FSparkUtils.addOrderedId(rdd);
            rddIndex.foreach(t -> logger.debug("{}", t));
            List<Long> idCollect = new ArrayList<>(rddIndex.map(Tuple2::_1).distinct().collect());
            int idSize = idCollect.size();
            assertEquals(size, idSize);

            idCollect.sort(Comparator.comparingLong(Long::longValue));
            for (int i = 0; i < idCollect.size(); i++) {
                assertEquals(i, idCollect.get(i).longValue());
            }
        }
    }

    @Test
    public void unionReduceLong() {
        JavaPairRDD<Integer, Long> rdd1 = rddOf(new Tuple2<>(1, 1L), new Tuple2<>(2, 2L)).mapToPair(t -> t);
        JavaPairRDD<Integer, Long> rdd2 = rddOf(new Tuple2<>(2, 1L), new Tuple2<>(1, 2L)).mapToPair(t -> t);

        Optional<JavaPairRDD<Integer, Long>> rdd = FSparkUtils.unionReduceLong(FUtils.listOf(rdd1, rdd2));

        assertTrue(rdd.isPresent());

        rdd.get().foreach(t -> assertEquals(3L, t._2.longValue()));
    }

    @Test
    public void union() {
        JavaRDD<Object> union = FSparkUtils.union(spark, Collections.emptyList());
        assertEquals(0L, union.count());
    }

    @Test
    public void union2() {
        List<FPair<Integer, JavaRDD<Integer>>> collect = IntStream.range(0, 10).mapToObj(ig -> {
            int num = random.nextInt(100);
            JavaRDD<Integer> rdd = sc.parallelize(IntStream.range(0, num).boxed().collect(Collectors.toList()));
            return new FPair<>(num, rdd);
        }).collect(Collectors.toList());

        int num = collect.stream().map(FPair::k).mapToInt(Integer::intValue).sum();
        List<JavaRDD<Integer>> rddList = collect.stream().map(FPair::v).collect(Collectors.toList());

        JavaRDD<Integer> union = FSparkUtils.union(spark, rddList);

        assertEquals(num, union.count());
    }
}