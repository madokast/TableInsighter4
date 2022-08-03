package com.sics.rock.tableinsight4.preprocessing.constant;

import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A container holds constant.
 *
 * @param <T> constant type
 * @author zhaorx
 */
public class FConstant<T> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FTableInfo.class);

    public static final long INDEX_OF_NULL = -1L;
    public static final long INDEX_NOT_FOUND = -2L;
    public static final long INDEX_NOT_INIT = -3L;
    public static final long INDEX_OF_POSITIVE_INFINITY = -4L;
    public static final long INDEX_OF_NEGATIVE_INFINITY = -5L;
    public static final long INDEX_OF_NAN = -6L;

    public static final FConstant NULL = new FConstant<>("null", INDEX_OF_NULL);
    public static final FConstant POSITIVE_INFINITY = new FConstant<>("Inf", INDEX_OF_POSITIVE_INFINITY);
    public static final FConstant NEGATIVE_INFINITY = new FConstant<>("-Inf", INDEX_OF_NEGATIVE_INFINITY);
    public static final FConstant NAN = new FConstant<>("NaN", INDEX_OF_NAN);

    private static final Map<Long, FConstant> SPECIAL_CONST_MAP = FTiUtils.mapOf(INDEX_OF_NULL, NULL,
            INDEX_OF_POSITIVE_INFINITY, POSITIVE_INFINITY,
            INDEX_OF_NEGATIVE_INFINITY, NEGATIVE_INFINITY,
            INDEX_OF_NAN, NAN);

    /**
     * Nullable
     */
    private final T constant;

    /**
     * lazy init
     * -x: special value
     * -3: not init
     * -2: not found
     * -1: null
     * >=0 : index for normal value
     */
    private long index = INDEX_NOT_INIT;

    public static <T> FConstant<T> of(T constant) {
        long index = specialIndexOf(constant);
        return SPECIAL_CONST_MAP.getOrDefault(index, new FConstant<>(constant));
    }

    public static long specialIndexOf(Object obj) {
        if (obj == null) return INDEX_OF_NULL;
        if (obj instanceof Double) {
            if (Double.isNaN((Double) obj)) return INDEX_OF_NAN;
            if (obj.equals(Double.POSITIVE_INFINITY)) return INDEX_OF_POSITIVE_INFINITY;
            if (obj.equals(Double.NEGATIVE_INFINITY)) return INDEX_OF_NEGATIVE_INFINITY;
        }

        if (obj instanceof Float) {
            if (Float.isNaN((Float) obj)) return INDEX_OF_NAN;
            if (obj.equals(Float.POSITIVE_INFINITY)) return INDEX_OF_POSITIVE_INFINITY;
            if (obj.equals(Float.NEGATIVE_INFINITY)) return INDEX_OF_NEGATIVE_INFINITY;
        }

        return INDEX_NOT_FOUND;
    }


    @Override
    public String toString() {
        String c = Objects.toString(constant);
        if (index == INDEX_NOT_INIT) {
            return c + "(un-index)";
        } else if (index == INDEX_NOT_FOUND) {
            return c + "(index not found)";
        } else return c + "(" + index + ")";
    }

    public T getConstant() {
        return constant;
    }

    public String toUserString() {
        return "'" + constant + "'";
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getIndex() {
        return index;
    }

    public boolean indexNotInit() {
        return index == INDEX_NOT_INIT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FConstant<?> fConstant = (FConstant<?>) o;
        if (this.constant == null) {
            return fConstant.constant == null && this.index == fConstant.index;
        } else {
            return this.constant.equals(fConstant.constant);
        }
    }

    @Override
    public int hashCode() {
        return constant == null ? Long.hashCode(index) : constant.hashCode();
    }

    private FConstant(T constant, long index) {
        this.constant = constant;
        this.index = index;
    }

    private FConstant(T constant) {
        this.constant = constant;
    }

    public static boolean normalValue(Object obj) {
        return specialIndexOf(obj) == INDEX_NOT_FOUND;
    }

    public boolean isComparable() {
        return index != INDEX_OF_NAN &&
                index != INDEX_OF_NULL &&
                constant instanceof Comparable;
    }
}
