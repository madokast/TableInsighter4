package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * utils of element-index in rdd
 *
 * @author zhaorx
 */
public class FRddElementIndexUtils {

    public static <T> JavaPairRDD<FRddElementIndex, T> elementIndexRDD(JavaRDD<T> rdd) {
        return rdd.mapPartitionsToPair(iter -> {
            final int pid = TaskContext.get().partitionId();
            int offset = 0;
            final List<Tuple2<FRddElementIndex, T>> results = new ArrayList<>();
            while (iter.hasNext()) {
                T object = iter.next();
                results.add(new Tuple2<>(new FRddElementIndex(pid, offset), object));
                ++offset;
            }
            return results.iterator();
        });
    }

    public static <T> JavaPairRDD<T, FRddElementIndex> rddElementIndex(JavaRDD<T> rdd) {
        return rdd.mapPartitionsToPair(iter -> {
            final int pid = TaskContext.get().partitionId();
            int offset = 0;
            final List<Tuple2<T, FRddElementIndex>> results = new ArrayList<>();
            while (iter.hasNext()) {
                T object = iter.next();
                results.add(new Tuple2<>(object, new FRddElementIndex(pid, offset)));
                ++offset;
            }
            return results.iterator();
        });
    }

    /**
     * utils
     */
    public static <T> List<T> filterElementsByIndices(JavaRDD<T> rdd, List<FRddElementIndex> indices) {
        final Map<Integer, Map<Integer, FRddElementIndex>> groupedIndices = indices.stream()
                .collect(Collectors.groupingBy(
                        FRddElementIndex::getPartitionId,
                        HashMap::new,
                        Collectors.toMap(FRddElementIndex::getOffset, Function.identity())
                ));

        final Map<FRddElementIndex, T> found = rdd.mapPartitionsToPair(iter -> {
            final int pid = TaskContext.get().partitionId();
            final Map<Integer, FRddElementIndex> offset2Index = groupedIndices.get(pid);
            final List<Tuple2<FRddElementIndex, T>> results = new ArrayList<>();
            int offset = 0;
            while (iter.hasNext()) {
                T object = iter.next();
                if (offset2Index.containsKey(offset)) {
                    results.add(new Tuple2<>(offset2Index.get(offset), object));
                }
                ++offset;
            }
            return results.iterator();
        }).collect().stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        return indices.stream().map(found::get).collect(Collectors.toList());
    }

}
