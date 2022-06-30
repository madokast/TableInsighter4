package com.sics.rock.tableinsight4.core.interval.bitset;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * BitSet
 * fixed-size & no check & cannot set false
 * impl by long[], the highest bit in long used as lowest bit in FBitSet
 * <p>
 * bs.set(0) -> 10000....
 * bs.set(3) -> 00010....
 *
 * @author zhaorx
 */
public class FBitSet implements Serializable {

    private static final long HIGHEST = 1L << 63;

    private final long[] words;

    /**
     * create a bitset with fixed-size
     */
    public FBitSet(int capacity) {
        int wordNumber = capacity / Long.SIZE;
        if (capacity % Long.SIZE != 0) ++wordNumber;
        words = new long[wordNumber];
    }

    /**
     * set i-th bit as true
     */
    public void set(int i) {
        int wi = i / Long.SIZE;
        i = i % Long.SIZE;
        words[wi] |= (HIGHEST >>> i);
    }

    /**
     * get the i-th bit
     */
    public boolean get(int i) {
        int wi = i / Long.SIZE;
        i = i % Long.SIZE;
        return (words[wi] & (HIGHEST >>> i)) != 0;
    }

    /**
     * If bitset a is a sub set of b, it means a & b == a
     * <p>
     * a: 1001
     * b: 1101
     * then a is a sub set of b
     *
     * @return this is sub set of 'maybe' or not
     */
    public boolean isSubSetOf(FBitSet maybe) {
        if (this == maybe) return true;

        int min = Math.min(words.length, maybe.words.length);
        for (int wi = 0; wi < min; wi++) {
            long and = words[wi] & maybe.words[wi];
            if (and != words[wi]) {
                return false;
            }
        }
        for (int i = min; i < words.length; i++) {
            if (words[i] != 0L) {
                return false;
            }
        }
        return true;
    }

    /**
     * iter-method:
     * for (int bit = bitSet.nextSetBit(0); bit >= 0; bit = bitSet.nextSetBit(bit + 1)) {}
     * the stream() method is recommended
     *
     * @param fromIncluding the index (including) begins search
     * @return next bit sit (true) index or -1 if not found
     */
    public int nextSetBit(int fromIncluding) {
        if (words.length == 0) return -1;
        // get the word of from
        int wordIndex = fromIncluding / Long.SIZE;
        if (wordIndex >= words.length) return -1;

        // get the local index in the word
        int bitIndex = fromIncluding % Long.SIZE;

        long cur;
        // find next in the word
        if (words[wordIndex] != 0) {
            cur = words[wordIndex];
            cur <<= bitIndex;
            if (cur != 0L) {
                return fromIncluding + Long.numberOfLeadingZeros(cur);
            }
        }

        // if not found int the word, goto next word
        wordIndex++;
        for (; wordIndex < words.length; wordIndex++) {
            cur = words[wordIndex];
            if (cur != 0L) {
                return wordIndex * Long.SIZE + Long.numberOfLeadingZeros(cur);
            }
        }

        return -1;
    }

    /**
     * iter-method:
     * for (int j = bs.previousSetBit(capacity); j >= 0; j = bs.previousSetBit(j - 1)) {}
     *
     * @param fromIncluding the index (including) begins search
     * @return previous bit sit (true) index or -1 if not found
     */
    public int previousSetBit(int fromIncluding) {
        if (words.length == 0) return -1;

        // word of from
        int wordIndex = fromIncluding / Long.SIZE;

        // if the word is out of bound, return highest
        if (wordIndex >= words.length) return highest();

        // the local index in the word
        int bitIndex = fromIncluding % Long.SIZE;

        long cur;
        // find in the word
        if (words[wordIndex] != 0) {
            cur = words[wordIndex];
            cur >>>= (Long.SIZE - bitIndex - 1);
            if (cur != 0L) {
                return fromIncluding - Long.numberOfTrailingZeros(cur);
            }
        }

        // if not found int the word, goto previous word
        wordIndex--;
        for (; wordIndex >= 0; wordIndex--) {
            cur = words[wordIndex];
            if (cur != 0L) {
                return (wordIndex + 1) * Long.SIZE - Long.numberOfTrailingZeros(cur) - 1;
            }
        }

        return -1;
    }

    /**
     * @return number of bit (which is true)
     */
    public int cardinality() {
        int c = 0;
        for (long word : words) {
            c += Long.bitCount(word);
        }
        return c;
    }

    /**
     * @return the highest bit (which is true) index
     */
    public int highest() {
        for (int i = words.length - 1; i >= 0; i--) {
            if (words[i] != 0L) {
                return (i + 1) * Long.SIZE - Long.numberOfTrailingZeros(words[i]) - 1;
            }
        }

        return -1;
    }

    /**
     * bitset a and b are equal if a & b = 0
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        long[] oWords = ((FBitSet) o).words;

        int length = Math.min(words.length, oWords.length);

        for (int i = 0; i < length; i++) {
            if (words[i] != oWords[i]) return false;
        }

        for (int i = length; i < words.length; i++) {
            if (words[0] != 0) return false;
        }

        for (int i = length; i < oWords.length; i++) {
            if (oWords[0] != 0) return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        long hash = 0;
        int i = words.length - 1;

        while (i >= 0 && words[i] == 0L) i--;

        for (; i >= 0; i--) {
            // cyclic shift
            hash ^= (words[i] >>> i) | (words[i] << (Long.SIZE - i));
        }

        hash ^= (hash >>> 32);

        return (int) hash;
    }

    private FBitSet(long[] words) {
        this.words = words;
    }

    public FBitSet copy() {
        return new FBitSet(Arrays.copyOf(words, words.length));
    }

    /**
     * create bitset by 01 string
     */
    public static FBitSet of(String s01) {
        FBitSet bs = new FBitSet(s01.length());
        for (int i = 0; i < s01.length(); i++) {
            if (s01.charAt(i) == '1') bs.set(i);
        }
        return bs;
    }

    // refer jdk8
    public IntStream stream() {
        class BitSetIterator implements PrimitiveIterator.OfInt {
            private int next = nextSetBit(0);

            @Override
            public boolean hasNext() {
                return next != -1;
            }

            @Override
            public int nextInt() {
                if (next != -1) {
                    int ret = next;
                    next = nextSetBit(next + 1);
                    return ret;
                } else {
                    throw new NoSuchElementException();
                }
            }
        }

        return StreamSupport.intStream(
                () -> Spliterators.spliterator(new BitSetIterator(), cardinality(),
                        Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED),
                Spliterator.SIZED | Spliterator.SUBSIZED |
                        Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.SORTED,
                false);
    }

    @Override
    public String toString() {
        return binaryContent() + " " + Arrays.toString(toArray());
    }

    public String binaryContent() {
        StringBuilder sb = new StringBuilder(words.length * Long.SIZE + 1);
        for (long word : words) {
            String str = Long.toBinaryString(word);
            for (int i = 0; i < Long.SIZE - str.length(); i++) {
                sb.append("0");
            }
            sb.append(str);
            sb.append("-");
        }
        return sb.substring(0, words.length * Long.SIZE);
    }

    public int[] toArray() {
        return stream().toArray();
    }

    public Set<Integer> toSet() {
        return stream().boxed().collect(Collectors.toSet());
    }

    public List<Integer> toList() {
        return stream().boxed().collect(Collectors.toList());
    }

    public long heapSize() {
        // size of this pointer = 8 markWord + 4 clsPtr + 4 longArr + 0 pad = 16 B
        long thisObj = 8 + 4 + 4;
        // size of long array = 8 markWord + 4 clsPtr + 4 int_for_length + 8 long + 0 pad
        long arrSize = 8 + 4 + 4 + Long.BYTES * words.length;
        return thisObj + arrSize;
    }

    public int size() {
        return words.length * Long.SIZE;
    }
}

