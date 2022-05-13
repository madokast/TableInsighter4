package com.sics.rock.tableinsight4.internal;


import org.junit.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class FPairTest {

    @Test
    public void test_pair_map() {

        final Map<Integer, Double> m1 = IntStream.range(0, 10).mapToObj(i -> new FPair<>(i, i * i * 1D))
                .collect(Collectors.toMap(FPair::k, FPair::v));

        final Map<Integer, Double> m2 = IntStream.range(0, 10).boxed().collect(Collectors.toMap(Function.identity(), i -> i * i * 1D));

        assertEquals(m2, m1);
    }

}