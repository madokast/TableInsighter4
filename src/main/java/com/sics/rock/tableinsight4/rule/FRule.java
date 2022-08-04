package com.sics.rock.tableinsight4.rule;

import com.sics.rock.tableinsight4.evidenceset.FIEvidenceSet;
import com.sics.rock.tableinsight4.internal.bitset.FBitSet;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Association Rule
 * <p>
 * this class is not serializable.
 * FRuleBodyTO is used for distribution of rules into executors
 *
 * @author zhaorx
 */
public class FRule {

    /**
     * the left hand side of rule
     */
    public final FBitSet xs;

    /**
     * the right hand side
     */
    public final int y;

    /**
     * support of the left hand side
     * es.filter(xs).count
     */
    public long xSupport = 0;

    /**
     * support of the rule
     * es.filter(xs & y).count
     */
    public long support = 0;

    public FRule(FBitSet xs, int y) {
        this.xs = xs;
        this.y = y;
    }

    /**
     * length of the left hand side
     */
    public int xsLength(FPredicateIndexer predicateIndexer) {
        return xs.stream().map(x -> predicateIndexer.getPredicate(x).length()).sum();
    }

    /**
     * for test and check
     */
    public boolean isAllZero() {
        return this.xSupport == 0L && this.support == 0L;
    }

    @Override
    public String toString() {
        return toString("→");
    }

    public String toString(String implicationSymbol) {
        return xs.binaryContent() + " " + implicationSymbol + " " + y + ", " + support + ", " + unSupport();
    }

    public String toString(FPredicateIndexer indexer, String conjunctionSymbol, String implicationSymbol) {
        String rhs = xs.stream().mapToObj(indexer::getPredicate).map(Object::toString).collect(Collectors.joining(" " + conjunctionSymbol + " "));
        return rhs + " " + implicationSymbol + " " + indexer.getPredicate(y).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FRule rule = (FRule) o;
        return y == rule.y &&
                Objects.equals(xs, rule.xs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(xs, y);
    }

    public long unSupport() {
        return xSupport - support;
    }

    public double confidence() {
        if (xSupport == 0L) return 0D;
        return ((double) support) / ((double) xSupport);
    }

    /**
     * support of right hand side
     */
    public long ySupport(FIEvidenceSet evidenceSet) {
        return evidenceSet.predicateSupport()[y];
    }
}
