package com.sics.rock.tableinsight4.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 *
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

}
