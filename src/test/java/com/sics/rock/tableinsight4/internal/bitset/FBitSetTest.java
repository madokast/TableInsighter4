package com.sics.rock.tableinsight4.internal.bitset;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;


public class FBitSetTest {

    private static final Logger logger = LoggerFactory.getLogger(FBitSetTest.class);

    @Test
    public void longStr() {
        logger.info(Long.toBinaryString(1));
        logger.info(Long.toBinaryString(2));
        logger.info(Long.toBinaryString(3));
        logger.info(Long.toBinaryString(4));
        logger.info(Long.toBinaryString(-1));
    }

    @Test
    public void test_new() {
        FBitSet b1 = new FBitSet(0);
        logger.info(b1.toString());

        b1 = new FBitSet(1);
        logger.info(b1.toString());

        b1 = new FBitSet(64);
        logger.info(b1.toString());

        b1 = new FBitSet(65);
        logger.info(b1.toString());
    }

    @Test
    public void test_new1() {
        for (int len = 1; len < 500; len++) {
            FBitSet b1 = new FBitSet(len);
            b1.set(len - 1);
            assertEquals(len - 1, b1.nextSetBit(0));
            assertEquals(1, b1.cardinality());
        }
    }

    @Test
    public void test_nextSetBit() {
        FBitSet b1 = FBitSet.of("001100");
        logger.info("nextSetBit " + b1.nextSetBit(0));
        logger.info("nextSetBit " + b1.nextSetBit(1));
        logger.info("nextSetBit " + b1.nextSetBit(2));
        logger.info("nextSetBit " + b1.nextSetBit(3));
        logger.info("nextSetBit " + b1.nextSetBit(4));
    }

    @Test
    public void test_nextSetBit1() {
        FBitSet b1 = new FBitSet(81);
        b1.set(0);
        b1.set(1);
        b1.set(80);
        logger.info("nextSetBit " + b1.nextSetBit(0));
        logger.info("nextSetBit " + b1.nextSetBit(1));
        logger.info("nextSetBit " + b1.nextSetBit(2));
        logger.info("nextSetBit " + b1.nextSetBit(3));
        logger.info("nextSetBit " + b1.nextSetBit(4));
        logger.info("nextSetBit " + b1.nextSetBit(79));
        logger.info("nextSetBit " + b1.nextSetBit(80));
        logger.info("nextSetBit " + b1.nextSetBit(81));
        logger.info("nextSetBit " + b1.nextSetBit(82));
    }

    @Test
    public void test1() {
        FBitSet b1 = FBitSet.of("001100");
        FBitSet b2 = FBitSet.of("0011");
        logger.info(b1.toString());
        logger.info(b2.toString());
    }

    @Test
    public void set() {
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int len = r.nextInt(500) + 10;
            FBitSet b = new FBitSet(len);
            for (int i1 = 0; i1 < 20; i1++) {
                int rand = r.nextInt(len - 1);
                if (!b.get(rand)) {
                    b.set(rand);
                    assertTrue(b.get(rand));
                }
            }
        }
    }

    @Test
    public void set1() {
        FBitSet set = new FBitSet(64);
        logger.info(set.toString());
        Random r = new Random();
        for (int i = 0; i < 20; i++) {
            int s = r.nextInt(64);
            set.set(s);
            logger.info("set {}, cap {} ,{}", s, set.cardinality(), set.toString());
        }
    }

    @Test
    public void get() {
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int len = r.nextInt(500) + 10;
            FBitSet b = new FBitSet(len);
            for (int i1 = 0; i1 < 20; i1++) {
                int rand = r.nextInt(len - 1);
                if (!b.get(rand)) {
                    b.set(rand);
                    assertTrue(b.get(rand));
                }
            }
        }


        for (int i = 0; i < 1000; i++) {
            int len = r.nextInt(500) + 10;
            FBitSet b = new FBitSet(len);
            for (int i1 = 0; i1 < 20; i1++) {
                int rand = r.nextInt(len - 1);
                if (!b.get(rand)) {
                    b.set(rand);
                    b.set(rand);
                    assertTrue(b.get(rand));
                }
            }
        }
    }

    @Test
    public void get1() {
        Random r = new Random();
        for (int i = 0; i < 1000; i++) {
            int len = r.nextInt(500) + 10;
            FBitSet b = new FBitSet(len);
            Set<Integer> set = new HashSet<>();
            for (int i1 = 0; i1 < 20; i1++) {
                set.add(r.nextInt(len));
            }
            assertFalse(set.isEmpty());
            for (Integer id : set) {
                b.set(id);
            }
            for (int i1 = 0; i1 < len; i1++) {
                if (set.contains(i1)) assertTrue(b.get(i1));
                else assertFalse(b.get(i1));
            }

            assertEquals(set.size(), b.cardinality());
        }
    }

    @Test
    public void isSubSetOf() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs = new FBitSet(cap);
            FBitSet sub = new FBitSet(cap);
            Set<Integer> set = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(1000) + 1).collect(Collectors.toSet());
            assertFalse(set.isEmpty());

            for (Integer integer : set) {
                bs.set(integer);
                if (Math.random() > 0.5) {
                    sub.set(integer);
                }
            }

            assertTrue(sub.cardinality() <= bs.cardinality());
            logger.info("{} < {}", sub.cardinality(), sub.cardinality());

            assertTrue(sub.isSubSetOf(bs));
            if (sub.cardinality() < bs.cardinality()) assertFalse(bs.isSubSetOf(sub));
        }
    }

    @Test
    public void cardinality() {
        FBitSet bs = new FBitSet(100);
        assertEquals(0, bs.cardinality());

        bs.set(0);
        assertEquals(1, bs.cardinality());
        bs.set(0);
        assertEquals(1, bs.cardinality());

        bs.set(1);
        assertEquals(2, bs.cardinality());

        bs.set(80);
        assertEquals(3, bs.cardinality());
        bs.set(80);
        assertEquals(3, bs.cardinality());

        for (int i = 0; i < 10; i++) {
            bs.set(i);
        }
        assertEquals(11, bs.cardinality());

        for (int i = 0; i < 50; i++) {
            bs.set(i);
        }
        assertEquals(51, bs.cardinality());

        bs.set(81);
        assertEquals(52, bs.cardinality());

        for (int i = 0; i < 95; i++) {
            bs.set(i);
        }
        assertEquals(95, bs.cardinality());

        for (int i = 0; i < 100; i++) {
            bs.set(i);
        }
        assertEquals(100, bs.cardinality());
    }

    @Test
    public void cardinality1() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs = new FBitSet(cap);
            Set<Integer> set = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(1000) + 1).collect(Collectors.toSet());
            assertFalse(set.isEmpty());

            set.forEach(bs::set);

            assertEquals(set.size(), bs.cardinality());
        }
    }

    @Test
    public void highest() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs = new FBitSet(cap);
            List<Integer> collect = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(50) + 1)
                    .distinct().sorted().collect(Collectors.toList());
            assertFalse(collect.isEmpty());
            collect.forEach(bs::set);

            assertEquals(collect.size(), bs.cardinality());

            int max = collect.get(collect.size() - 1);
            assertEquals(max, bs.highest());

            assertEquals(-1, bs.nextSetBit(max + 1));
        }
    }

    @Test
    public void testEquals() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs1 = new FBitSet(cap);
            FBitSet bs2 = new FBitSet(cap);

            List<Integer> collect = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(5) + 1)
                    .distinct().sorted().collect(Collectors.toList());
            assertFalse(collect.isEmpty());

            Collections.shuffle(collect);
            collect.forEach(bs1::set);
            Collections.shuffle(collect);
            collect.forEach(bs2::set);

            assertEquals(bs1, bs2);

            Collections.sort(collect);

            assertEquals(collect, bs1.toList());
            assertEquals(collect, bs2.toList());

        }
    }

    @Test
    public void testHashCode() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs1 = new FBitSet(cap);
            FBitSet bs2 = new FBitSet(cap);

            List<Integer> collect = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(5) + 1)
                    .distinct().sorted().collect(Collectors.toList());
            assertFalse(collect.isEmpty());

            Collections.shuffle(collect);
            collect.forEach(bs1::set);
            Collections.shuffle(collect);
            collect.forEach(bs2::set);

            assertEquals(bs1.hashCode(), bs2.hashCode());
        }
    }

    @Test
    public void copy() {

        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int cap = random.nextInt(5000) + 10;
            FBitSet bs1 = new FBitSet(cap);

            List<Integer> collect = Stream.generate(() -> random.nextInt(cap)).limit(random.nextInt(10000) + 1)
                    .distinct().sorted().collect(Collectors.toList());
            assertFalse(collect.isEmpty());

            Collections.shuffle(collect);
            collect.forEach(bs1::set);

            FBitSet bs2 = bs1.copy();

            assertEquals(bs1.hashCode(), bs2.hashCode());
        }

    }

    @Test
    public void of() {
        FBitSet of = FBitSet.of("111000111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
        logger.info(of.toString());
    }

    @Test
    public void stream() {
        FBitSet of = FBitSet.of("111000111111111111111111111111111111111111111111111111111111111111111111111111111111111111111");
        int[] streamArr = of.stream().toArray();

        List<Integer> bits = new ArrayList<>();
        for (int bit = of.nextSetBit(0); bit >= 0; bit = of.nextSetBit(bit + 1)) {
            bits.add(bit);
        }

        assertArrayEquals(bits.stream().mapToInt(Integer::intValue).toArray(), streamArr);
    }

    @Test
    public void testToString() {
        FBitSet s1 = FBitSet.of("11100000000000000000000000000000000000000000000000000000000000011110000000000000000000000000000000000000000000000000000000000001");
        logger.info(s1.toString());
        logger.info(s1.binaryContent());
        logger.info(s1.toList().toString());
    }

    @Test
    public void testToString2() {
        FBitSet s1 = FBitSet.of("111000000000000000000000000000000000000000000000000000000000000111100000000000000000000000000000000000000000000000000000000000011");
        logger.info(s1.toString());
        logger.info(s1.binaryContent());
        logger.info(s1.toList().toString());
    }

    @Test
    public void isSubSetOf1() {
        FBitSet s1 = FBitSet.of("111");
        FBitSet s2 = FBitSet.of("");
        logger.info("s1 = {}", s1.toString());
        logger.info("s2 = {}", s2.toString());
        assertTrue(s2.isSubSetOf(s1));

        assertEquals(-1, s2.nextSetBit(0));
        assertEquals(-1, s2.highest());
    }

    @Test
    public void nextSetBit() {
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            int capacity = 1000 + r.nextInt(200);
            FBitSet bs = new FBitSet(capacity);
            List<Integer> set = Stream.generate(() -> r.nextInt(capacity)).limit(20 + i)
                    .peek(bs::set)
                    .distinct().collect(Collectors.toList());
            int size = set.size();
            logger.info("size = {}", size);
            set.sort(Comparator.comparingInt(Integer::intValue));

            List<Integer> look = new ArrayList<>();
            for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j + 1)) {
                look.add(j);
            }
            assertEquals(set, look);


            Collections.reverse(set);
            look.clear();
            for (int j = bs.previousSetBit(capacity); j >= 0; j = bs.previousSetBit(j - 1)) {
                look.add(j);
            }
            assertEquals(set, look);

        }
    }

    @Test
    public void nextSetBit1() {
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            int capacity = 10000 + r.nextInt(2000);
            FBitSet bs = new FBitSet(capacity);
            List<Integer> set = Stream.generate(() -> r.nextInt(capacity)).limit(200 + i)
                    .peek(bs::set)
                    .distinct().collect(Collectors.toList());
            int size = set.size();
            logger.info("size = {}", size);
            set.sort(Comparator.comparingInt(Integer::intValue));

            List<Integer> look = new ArrayList<>();
            for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j + 1)) {
                look.add(j);
            }
            assertEquals(set, look);


            Collections.reverse(set);
            look.clear();
            for (int j = bs.previousSetBit(capacity); j >= 0; j = bs.previousSetBit(j - 1)) {
                look.add(j);
            }
            assertEquals(set, look);

        }
    }

    @Test
    public void nextSetBit2() {
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            int capacity = 100 + i;
            FBitSet bs = new FBitSet(capacity);
            List<Integer> set = Stream.generate(() -> r.nextInt(capacity)).limit(200000 + i)
                    .peek(bs::set)
                    .distinct().collect(Collectors.toList());
            int size = set.size();
            logger.info("size = {}", size);
            set.sort(Comparator.comparingInt(Integer::intValue));

            List<Integer> look = new ArrayList<>();
            for (int j = bs.nextSetBit(0); j >= 0; j = bs.nextSetBit(j + 1)) {
                look.add(j);
            }
            assertEquals(set, look);


            Collections.reverse(set);
            look.clear();
            for (int j = bs.previousSetBit(capacity); j >= 0; j = bs.previousSetBit(j - 1)) {
                look.add(j);
            }
            assertEquals(set, look);

        }
    }
}