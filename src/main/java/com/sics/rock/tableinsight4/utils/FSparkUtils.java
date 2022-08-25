package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.partitioner.FPrePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author zhaorx
 */
public class FSparkUtils {

    private static final Logger logger = LoggerFactory.getLogger(FSparkUtils.class);

    /**
     * create a local spark session
     */
    public static SparkSession localSparkSession(String... keyValues) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("local-spark-app");
        for (int i = 0; i < keyValues.length; i += 2) {
            conf.set(keyValues[i], keyValues[i + 1]);
        }
        return SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Swap the keys (MK, RK) to (RK, MK)
     * For example
     * a -> {b -> 3}
     * swap the key a,b
     * b -> {a -> 3}
     */
    public static <MK, RK, V> JavaPairRDD<RK, Map<MK, V>> swapKey(Map<MK, JavaPairRDD<RK, V>> rddMap) {
        // 1. Put MK into RDD.
        // RK -> {MK, V}
        final List<JavaPairRDD<RK, Tuple2<MK, V>>> RDDs = rddMap.entrySet().stream().map(e -> {
            final MK mk = e.getKey();
            return e.getValue().mapToPair(t -> new Tuple2<>(t._1, new Tuple2<>(mk, t._2)));
        }).collect(Collectors.toList());

        // 2.Union all RDDs
        JavaPairRDD<RK, Tuple2<MK, V>> unionRDD = RDDs.get(0);
        for (int i = 1; i < RDDs.size(); i++) {
            unionRDD = unionRDD.union(RDDs.get(i));
        }
        unionRDD = unionRDD.cache().setName("swapKey_" + UUID.randomUUID());

        // 3. Get the number of RK keys
        final List<RK> keys = unionRDD.map(t -> t._1).distinct().collect();

        // 4. Each RK key creates a partition
        return unionRDD.partitionBy(new FPrePartitioner(keys)).mapPartitionsToPair(iter -> {
            RK rk = null;
            Map<MK, V> map = new HashMap<>();
            while (iter.hasNext()) {
                final Tuple2<RK, Tuple2<MK, V>> tt = iter.next();
                if (rk == null) rk = tt._1;
                FAssertUtils.require(_rk -> _rk.equals(tt._1),
                        _rk -> "SwapKey error RK[" + _rk + "] != [" + tt._1 + "]", rk);
                final Tuple2<MK, V> kv = tt._2;
                map.put(tt._2._1, kv._2);
            }
            if (rk == null) return Collections.emptyIterator();
            else return Collections.singletonList(new Tuple2<>(rk, map)).iterator();
        });
    }

    /**
     * ordered index of rdd
     */
    public static <E> JavaPairRDD<E, Long> orderedIndex(JavaRDD<E> rdd) {
        // Get partition info
        final List<Tuple2<Integer, Long>> partitionInfo = rdd.mapPartitionsToPair(iter -> {
            long count = 0L;
            while (iter.hasNext()) {
                iter.next();
                ++count;
            }
            return Collections.singletonList(new Tuple2<>(TaskContext.get().partitionId(), count)).iterator();
        }).sortByKey().collect();

        // Calculate offsets in each partition
        final int partitionSize = partitionInfo.size();
        long[] partitionIndexOffset = new long[partitionSize];
        partitionIndexOffset[0] = 0L;
        for (int pid = 1; pid < partitionSize; pid++) {
            partitionIndexOffset[pid] = partitionIndexOffset[pid - 1] + partitionInfo.get(pid - 1)._2;
        }

        return rdd.mapPartitionsToPair(iter -> {
            final int pid = TaskContext.get().partitionId();
            final long offset = partitionIndexOffset[pid];
            long count = 0L;

            final List<Tuple2<E, Long>> list = new ArrayList<>(partitionInfo.get(pid)._2.intValue());
            while (iter.hasNext()) {
                final E next = iter.next();
                list.add(new Tuple2<>(next, count + offset));
                ++count;
            }
            return list.iterator();
        });
    }

    /**
     * ordered index the rdd
     */
    public static <E> JavaPairRDD<Long, E> addOrderedId(JavaRDD<E> rdd) {
        final List<Tuple2<Integer, Long>> partitionInfo = rdd.mapPartitionsToPair(iter -> {
            long count = 0L;
            while (iter.hasNext()) {
                iter.next();
                ++count;
            }
            return Collections.singletonList(new Tuple2<>(TaskContext.get().partitionId(), count)).iterator();
        }).sortByKey().collect();

        final int partitionSize = partitionInfo.size();
        long[] partitionIndexOffset = new long[partitionSize];
        partitionIndexOffset[0] = 0L;
        for (int pid = 1; pid < partitionSize; pid++) {
            partitionIndexOffset[pid] = partitionIndexOffset[pid - 1] + partitionInfo.get(pid - 1)._2;
        }

        return rdd.mapPartitionsToPair(iter -> {
            final int pid = TaskContext.get().partitionId();
            final long offset = partitionIndexOffset[pid];
            long count = 0L;

            final List<Tuple2<Long, E>> list = new ArrayList<>(partitionInfo.get(pid)._2.intValue());
            while (iter.hasNext()) {
                final E next = iter.next();
                list.add(new Tuple2<>(count + offset, next));
                ++count;
            }
            return list.iterator();
        });
    }

    public static <K> Optional<JavaPairRDD<K, Long>> unionReduceLong(List<JavaPairRDD<K, Long>> RDDs) {
        return FTiUtils.mergeReduce(RDDs, (r1, r2) -> r1.union(r2).reduceByKey(Long::sum));
    }

    public static <E> JavaRDD<E> union(SparkSession spark, List<JavaRDD<E>> RDDs) {
        if (RDDs.isEmpty()) return JavaSparkContext.fromSparkContext(spark.sparkContext()).emptyRDD();
        JavaRDD<E> one = RDDs.get(0);
        for (int i = 1; i < RDDs.size(); i++) {
            one = one.union(RDDs.get(i));
        }
        return one;
    }

    public static <E> JavaRDD<E> rddOf(List<E> elements, SparkSession spark) {
        final JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        return sc.parallelize(elements);
    }
}
