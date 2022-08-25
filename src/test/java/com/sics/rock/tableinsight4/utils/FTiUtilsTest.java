package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.test.env.FBasicTestEnv;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class FTiUtilsTest extends FBasicTestEnv {

    @Test
    public void test_createUnionFindSet() {
        final List<FPair<String, String>> pairs = FTiUtils.listOf(
                new FPair<>("a", "b")
        );

        final Map<String, Long> unionFindSet = FTiUtils.createUnionFindSet(pairs);

        assertEquals(unionFindSet.get("a"), unionFindSet.get("b"));
    }

    @Test
    public void test_createUnionFindSet2() {
        final List<FPair<String, String>> pairs = FTiUtils.listOf(
                new FPair<>("a", "b"),
                new FPair<>("c", "d")
        );

        final Map<String, Long> unionFindSet = FTiUtils.createUnionFindSet(pairs);

        assertEquals(unionFindSet.get("a"), unionFindSet.get("b"));
        assertEquals(unionFindSet.get("c"), unionFindSet.get("d"));

        assertNotEquals(unionFindSet.get("a"), unionFindSet.get("c"));
        assertNotEquals(unionFindSet.get("b"), unionFindSet.get("c"));
        assertNotEquals(unionFindSet.get("a"), unionFindSet.get("d"));
        assertNotEquals(unionFindSet.get("b"), unionFindSet.get("d"));
    }

    @Test
    public void test_createUnionFindSet3() {
        final List<FPair<String, String>> pairs = FTiUtils.listOf(
                new FPair<>("a", "b"),
                new FPair<>("c", "d"),
                new FPair<>("a", "c")
        );

        final Map<String, Long> unionFindSet = FTiUtils.createUnionFindSet(pairs);

        assertEquals(unionFindSet.get("a"), unionFindSet.get("b"));
        assertEquals(unionFindSet.get("c"), unionFindSet.get("d"));

        assertEquals(unionFindSet.get("a"), unionFindSet.get("c"));
        assertEquals(unionFindSet.get("b"), unionFindSet.get("c"));
        assertEquals(unionFindSet.get("a"), unionFindSet.get("d"));
        assertEquals(unionFindSet.get("b"), unionFindSet.get("d"));
    }

    @Test
    public void test_slice1() {
        List<Integer> list = IntStream.range(0, 100).boxed().collect(Collectors.toList());

        for (List<Integer> sl : FTiUtils.slice(list, 10)) {
            assertTrue(sl.size() <= 10);
            logger.info("slice = " + sl);
        }

        for (List<Integer> sl : FTiUtils.slice(list, 11)) {
            assertTrue(sl.size() <= 11);
            logger.info("slice = " + sl);
        }

        for (List<Integer> sl : FTiUtils.slice(list, 10)) {
            assertTrue(sl.size() <= 10);
            logger.info("slice = " + sl);
        }

        for (List<Integer> sl : FTiUtils.slice(list, 33)) {
            assertTrue(sl.size() <= 33);
            logger.info("slice = " + sl);
        }
    }
}