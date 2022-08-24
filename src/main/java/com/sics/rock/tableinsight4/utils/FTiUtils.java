package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * small utils
 *
 * @author zhaorx
 */
public class FTiUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTiUtils.class);

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

    public static <E> Map<E, Long> createUnionFindSet(List<FPair<E, E>> unionCases) {
        final List<E> allElements = unionCases.stream().flatMap(p -> Stream.of(p._k, p._v))
                .distinct().collect(Collectors.toList());

        Map<E, Integer> elementIndex = new HashMap<>();
        for (int i = 0; i < allElements.size(); i++) {
            elementIndex.put(allElements.get(i), i);
        }

        final List<FPair<Integer, Integer>> unionIndexes = unionCases.stream()
                .filter(p -> !p._k.equals(p._v))
                .map(p -> new FPair<>(elementIndex.get(p._k), elementIndex.get(p._v))).collect(Collectors.toList());


        final int[] ufs = createUnionFindSet(unionIndexes, allElements.size());

        Map<E, Long> result = new HashMap<>();

        for (int i = 0; i < ufs.length; i++) {
            int root = ufs[i];
            while (root != ufs[root]) root = ufs[root];
            result.put(allElements.get(i), (long) root);
        }

        return result;
    }

    private static int[] createUnionFindSet(List<FPair<Integer, Integer>> unionCases, int size) {

        class UFS {
            private int[] arr;

            private UFS(int size) {
                this.arr = new int[size];
                for (int i = 0; i < this.arr.length; i++) {
                    this.arr[i] = i;
                }
            }

            private int find(int e) {
                if (e == arr[e]) return e;
                else {
                    arr[e] = find(arr[e]);
                    return arr[e];
                }
            }

            private void union(int k, int v) {
                arr[find(k)] = find(v);
            }
        }

        final UFS ufs = new UFS(size);

        // union
        unionCases.forEach(pair -> ufs.union(pair._k, pair._v));

        return ufs.arr;
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

    @SafeVarargs
    public static <E> Set<E> setOf(E... es) {
        return new HashSet<>(listOf(es));
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

    public static <T> Map<T, Integer> indexList(List<T> list) {
        FAssertUtils.require(() -> list.stream().distinct().count() == (long) list.size(),
                () -> "Cannot index a list containing duplicate elements. " + list);

        Map<T, Integer> m = new HashMap<>();

        for (int i = 0; i < list.size(); i++) {
            m.put(list.get(i), i);
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

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> mapOf(K k1, V v1, K k2, V v2, K k3, V v3, Object... kvs) {
        int kvLen = kvs.length;
        FAssertUtils.require(kvLen % 2 == 0, "kvs.len " + kvLen + " is odd");
        Map<K, V> map = mapOf(k1, v1, k2, v2, k3, v3);
        for (int i = 0; i < kvLen; i += 2) {
            map.put((K) kvs[i], (V) kvs[i + 1]);
        }
        return map;
    }

    /**
     * @param maxDecimalPlace do not round when -1
     */
    public static String round(double number, int maxDecimalPlace, boolean allowExponentialForm) {
        final String s;
        if (allowExponentialForm || Double.isNaN(number) || Double.isInfinite(number)) {
            s = Double.toString(number);
        } else {
            final String temp = Double.toString(number).toUpperCase();
            s = temp.contains("E") ? new BigDecimal(number, MathContext.UNLIMITED).toPlainString() : temp;
        }
        if (maxDecimalPlace < 0) return s;

        final int dot = s.indexOf(".");
        if (dot == -1) return s;
        int end = dot + 1;
        while (end < s.length() && Character.isDigit(s.charAt(end))) ++end;

        if (maxDecimalPlace == 0) return s.substring(0, dot) + s.substring(end);
        else if (end - dot < maxDecimalPlace + 1) return s;
        else return s.substring(0, dot + maxDecimalPlace + 1) + s.substring(end);
    }

    public static long[] longArrSum(final long[] arr1, final long[] arr2) {
        FAssertUtils.require(arr1.length == arr2.length,
                "Element-wise adding of 2 long array with different length " + arr1.length + " and " + arr2.length);
        long[] r = Arrays.copyOf(arr1, arr1.length);
        for (int i = 0; i < r.length; i++) {
            r[i] += arr2[i];
        }
        return r;
    }

    public static <E extends Comparable<? super E>> boolean isOrdered(List<E> list) {
        for (int i = 1; i < list.size(); i++) {
            if (list.get(0).compareTo(list.get(1)) < 0) return false;
        }
        return true;
    }
}
