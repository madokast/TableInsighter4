package com.sics.rock.tableinsight4.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BinaryOperator;

/**
 * @author zhaorx
 */
public class FUtils {

    private static final Logger logger = LoggerFactory.getLogger(FUtils.class);

    public static <T> Optional<T> mergeReduce(List<T> array, BinaryOperator<T> op) {
        if (array.isEmpty()) return Optional.empty();
        return Optional.of(mergeReduce0(array, op, 0, array.size()));
    }

    private static <T> T mergeReduce0(List<T> array, BinaryOperator<T> op, int startIn, int endEx) {
        int length = endEx - startIn;
        if (length == 1) return array.get(startIn);
        else if (length == 2) return op.apply(array.get(startIn), array.get(startIn + 1));
        else return op.apply(
                    mergeReduce0(array, op, startIn, startIn + length / 2),
                    mergeReduce0(array, op, startIn + length / 2, endEx)
            );
    }

    public static <E> List<E> collect(Iterator<E> iter) {
        List<E> list = new ArrayList<>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        return list;
    }

    @SafeVarargs
    public static <E> List<E> listOf(E... es) {
        return Arrays.asList(es);
    }

    public static <T> Map<T, Integer> indexArray(T[] array) {

        FAssertUtils.require(() -> Arrays.stream(array).distinct().count() == (long) array.length,
                () -> "Cannot index an array containing duplicate elements. " + Arrays.toString(array));

        Map<T, Integer> m = new HashMap<>();

        for (int i = 0; i < array.length; i++) {
            m.put(array[i], i);
        }

        return m;
    }

    public static <K, V> Map<K, V> mapOf(K k, V v) {
        Map<K, V> map = new HashMap<>();
        map.put(k, v);
        return map;
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2) {
        Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
        Map<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return map;
    }
}
