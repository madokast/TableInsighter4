package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FPair;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FTiUtilsTest {

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

}