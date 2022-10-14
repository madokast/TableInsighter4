package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;
import java.util.Comparator;

/**
 * serializable comparator interface
 *
 * @author zhaorx
 */
public interface FSerializableComparator<T> extends Comparator<T>, Serializable {

    public static final FSerializableComparator<Long> LONG_COMPARATOR = Long::compareTo;

}
