package com.sics.rock.tableinsight4.evidenceset.predicateset;

import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Used only in evidence set
 *
 * @author zhaorx
 */
public class FPredicateSet implements FIPredicateSet {

    private final FBitSet bitSet;

    private final long support;

    public FPredicateSet(FBitSet bitSet, long support) {
        this.bitSet = bitSet;
        this.support = support;
    }

    @Override
    public FBitSet getBitSet() {
        return bitSet;
    }

    @Override
    public long getSupport() {
        return support;
    }

    @Override
    public String toString() {
        return bitSet.binaryContent() + "↑" + support;
    }

    public String toString(int predicateSize) {
        return bitSet.binaryContent().substring(0, predicateSize) + "↑" + support;
    }

    @Override
    public String toString(FPredicateIndexer indexProvider) {
        String psStr = bitSet.stream().mapToObj(indexProvider::getPredicate).map(Objects::toString)
                .collect(Collectors.joining(" ", "[", "]"));
        return psStr + "↑" + support;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FPredicateSet that = (FPredicateSet) o;
        return bitSet.equals(that.bitSet);
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }
}
