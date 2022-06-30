package com.sics.rock.tableinsight4.internal.bitset;

import com.sics.rock.tableinsight4.internal.FPair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A set stored bit-sets
 * The set is constructed as a tree to ease the search of subset.
 * <p>
 * tree-set of 101
 * []->(X)[0]
 * [0]->(X)[2]
 * [02]->(O)[]
 * <p>
 * tree-set of 101 and 1
 * []->(X)[0]
 * [0]->(O)[2]
 * [02]->(O)[]
 * <p>
 * tree-set of 1, 101, 101000001
 * []->(X)[0]
 * [0]->(O)[2]
 * [02]->(O)[8]
 * [028]->(O)[]
 * <p>
 * tree-set of 001, 0000001, 0100000001
 * []->(X)[1,2,6]
 * [1]->(X)[9]
 * [19]->(O)[]
 * [2]->(O)[]
 * [6]->(O)[]
 */
public class FBitSetSearchTree {

    private final Map<Integer, FBitSetSearchTree> subTrees = new HashMap<>();

    private boolean contain = false;

    public void add(FBitSet bitSet) {
        int next = -1;
        FBitSetSearchTree cur = this;

        for (; ; ) {
            next = bitSet.nextSetBit(next + 1);
            if (next < 0) {
                cur.contain = true;
                return;
            } else {
                FBitSetSearchTree subTree = cur.subTrees.getOrDefault(next, new FBitSetSearchTree());
                cur.subTrees.put(next, subTree);
                cur = subTree;
            }
        }
    }

    /**
     * @return is there an element in the tree, being the subset of target
     */
    public boolean containSubSet(FBitSet target) {
        return containSubSet(target, 0);
    }

    private boolean containSubSet(FBitSet bitSet, int searchFrom) {
        if (contain) return true;

        for (int next = bitSet.nextSetBit(searchFrom); next >= 0; next = bitSet.nextSetBit(next + 1)) {
            FBitSetSearchTree subTree = subTrees.get(next);
            if (subTree != null) {
                if (subTree.containSubSet(bitSet, next + 1)) return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        List<String> bitSets = new ArrayList<>();

        Deque<FPair<FBitSetSearchTree, List<Integer>>> queue = new LinkedList<>();
        queue.addLast(new FPair<>(this, new ArrayList<>()));

        while (!queue.isEmpty()) {
            FPair<FBitSetSearchTree, List<Integer>> first = queue.pollFirst();
            FBitSetSearchTree node = first.k();
            if (node.contain) {
                bitSets.add(FBitSet.of(first._v).toBriefString());
            }
            for (Map.Entry<Integer, FBitSetSearchTree> entry : node.subTrees.entrySet()) {
                ArrayList<Integer> nextBS = new ArrayList<>(first._v);
                FPair<FBitSetSearchTree, List<Integer>> next = new FPair<>(entry.getValue(), nextBS);
                nextBS.add(entry.getKey());

                queue.addLast(next);
            }
        }

        return bitSets.toString();
    }

    public List<String> structure() {
        return structure("[");
    }

    private String nodeStructure() {
        String tree = subTrees.keySet().stream().sorted().map(String::valueOf)
                .collect(Collectors.joining(",", "[", "]"));
        return (contain ? "(O)" : "(X)") + tree;
    }

    private List<String> structure(String prefix) {
        List<String> r = new ArrayList<>();
        r.add(prefix + "]->" + nodeStructure());
        for (Map.Entry<Integer, FBitSetSearchTree> entry : subTrees.entrySet()) {
            FBitSetSearchTree subTree = entry.getValue();
            Integer i = entry.getKey();
            r.addAll(subTree.structure(prefix + i));
        }
        return r;
    }

}
