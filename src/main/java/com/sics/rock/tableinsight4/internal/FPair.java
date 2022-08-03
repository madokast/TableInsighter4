package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;
import java.util.Objects;

/**
 * Same as javafx.util.Pair.
 * Some jdks do not contain javafx package,
 * so implement it in case.
 *
 * @author zhaorx
 */
public class FPair<K, V> implements Serializable {

    public final K _k;

    public final V _v;

    public FPair(K k, V v) {
        this._k = k;
        this._v = v;
    }

    public K k() {
        return _k;
    }

    public V v() {
        return _v;
    }

    @Override
    public String toString() {
        return "[" + _k +
                ", " + _v +
                ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FPair<?, ?> fPair = (FPair<?, ?>) o;
        return Objects.equals(_k, fPair._k) &&
                Objects.equals(_v, fPair._v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_k, _v);
    }
}
