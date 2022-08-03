package com.sics.rock.tableinsight4.internal.bitset;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FBitSetSearchTreeTest {
    private static final Logger logger = LoggerFactory.getLogger(FBitSetSearchTreeTest.class);

    @Test
    public void test() {
        FBitSet of = FBitSet.of("101");
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(of);
        for (String s : tree.structure()) {
            logger.info(s);
        }

        logger.info(tree.toString());
    }

    @Test
    public void test1() {
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(FBitSet.of("101"));
        tree.add(FBitSet.of("1"));
        for (String s : tree.structure()) {
            logger.info(s);
        }

        logger.info(tree.toString());
    }

    @Test
    public void test2() {
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(FBitSet.of("1"));
        tree.add(FBitSet.of("101"));
        for (String s : tree.structure()) {
            logger.info(s);
        }
    }

    @Test
    public void test3() {
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(FBitSet.of("1"));
        tree.add(FBitSet.of("101"));

        for (String s : tree.structure()) {
            logger.info(s);
        }

        assertTrue(tree.containSubSet(FBitSet.of("1")));
        assertTrue(tree.containSubSet(FBitSet.of("11")));
        assertTrue(tree.containSubSet(FBitSet.of("101")));
        assertTrue(tree.containSubSet(FBitSet.of("111")));
        assertTrue(tree.containSubSet(FBitSet.of("1110")));
        assertTrue(tree.containSubSet(FBitSet.of("1110111")));
        assertTrue(tree.containSubSet(FBitSet.of("1111111")));

        assertFalse(tree.containSubSet(FBitSet.of("011")));
        assertFalse(tree.containSubSet(FBitSet.of("010")));
    }

    @Test
    public void test4() {
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(FBitSet.of("1"));
        tree.add(FBitSet.of("101"));
        tree.add(FBitSet.of("1010000010"));

        for (String s : tree.structure()) {
            logger.info(s);
        }

        assertTrue(tree.containSubSet(FBitSet.of("1")));
        assertTrue(tree.containSubSet(FBitSet.of("101")));
        assertTrue(tree.containSubSet(FBitSet.of("101000001")));
        assertTrue(tree.containSubSet(FBitSet.of("11")));
        assertTrue(tree.containSubSet(FBitSet.of("1111")));
        assertTrue(tree.containSubSet(FBitSet.of("111111")));
        assertTrue(tree.containSubSet(FBitSet.of("11111111")));
        assertTrue(tree.containSubSet(FBitSet.of("1011")));
        assertTrue(tree.containSubSet(FBitSet.of("101000001111")));

        logger.info(tree.toString());
    }

    @Test
    public void test5() {
        FBitSetSearchTree tree = new FBitSetSearchTree();
        tree.add(FBitSet.of("001"));
        tree.add(FBitSet.of("0000001"));
        tree.add(FBitSet.of("0100000001"));

        for (String s : tree.structure()) {
            logger.info(s);
        }

        assertTrue(tree.containSubSet(FBitSet.of("001")));
        assertTrue(tree.containSubSet(FBitSet.of("0011")));
        assertTrue(tree.containSubSet(FBitSet.of("00111")));
        assertTrue(tree.containSubSet(FBitSet.of("001111")));
        assertTrue(tree.containSubSet(FBitSet.of("101")));
        assertTrue(tree.containSubSet(FBitSet.of("111")));


        assertTrue(tree.containSubSet(FBitSet.of("0000001")));
        assertTrue(tree.containSubSet(FBitSet.of("00000011")));
        assertTrue(tree.containSubSet(FBitSet.of("1000001")));
        assertTrue(tree.containSubSet(FBitSet.of("0100001")));
        assertTrue(tree.containSubSet(FBitSet.of("0010001")));

        assertTrue(tree.containSubSet(FBitSet.of("0100000001")));
        assertTrue(tree.containSubSet(FBitSet.of("0110000001")));
        assertTrue(tree.containSubSet(FBitSet.of("0101000001")));
        assertTrue(tree.containSubSet(FBitSet.of("0100100001")));
        assertTrue(tree.containSubSet(FBitSet.of("0100010001")));

        assertFalse(tree.containSubSet(FBitSet.of("0000000001")));
        assertFalse(tree.containSubSet(FBitSet.of("0100000000")));

        logger.info(tree.toString());
    }
}