package com.sics.rock.tableinsight4.evidenceset.predicateset;

import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * PredicateSet with support example (in rowIds)
 * For positive and negative examples
 *
 * @author zhaorx
 */
public class FExamplePredicateSet extends FPredicateSet {

    private final List<FRddElementIndex[]> supportIdsList = new ArrayList<>();

    public FExamplePredicateSet(FBitSet bitSet, long support) {
        super(bitSet, support);
    }

    public void add(FRddElementIndex[] supportIds) {
        this.supportIdsList.add(supportIds);
    }

    public void addAll(Collection<FRddElementIndex[]> supportIdsList) {
        this.supportIdsList.addAll(supportIdsList);
    }

    public List<FRddElementIndex[]> getSupportIdsList() {
        return supportIdsList;
    }

    public int supportRowIdsListSize() {
        return supportIdsList.size();
    }

    private String rowIdInfo() {
        return supportIdsList.stream().map(arr ->
                Arrays.stream(arr).map(Objects::toString).collect(Collectors.joining("x", "(", ")"))
        ).collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return super.toString() + "[" + rowIdInfo() + "]";
    }

    @Override
    public String toString(int predicateSize) {
        return super.toString(predicateSize) + "[" + rowIdInfo() + "]";
    }

    @Override
    public String toString(FPredicateIndexer indexProvider) {
        return super.toString(indexProvider) + "[" + rowIdInfo() + "]";
    }
}
