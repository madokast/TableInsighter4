package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;

/**
 * Same as javafx.util.Pair.
 * Some jdks do not contain javafx package.
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

}
