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

    // the negative index represents special value.
    public static final long INDEX_OF_NULL = -1L;
    public static final long INDEX_NOT_FOUND = -2L;
    public static final long INDEX_NOT_INIT = -3L;
    public static final long INDEX_OF_POSITIVE_INFINITY = -4L;
    public static final long INDEX_OF_NEGATIVE_INFINITY = -5L;
    public static final long INDEX_OF_NAN = -6L;

    public static final FConstant NULL = new FConstant<>(FSpecialVal.NULL, INDEX_OF_NULL);
    public static final FConstant<Comparable> POSITIVE_INFINITY = new FConstant<>(FSpecialVal.POSITIVE_INFINITY, INDEX_OF_POSITIVE_INFINITY);
    public static final FConstant<Comparable> NEGATIVE_INFINITY = new FConstant<>(FSpecialVal.NEGATIVE_INFINITY, INDEX_OF_NEGATIVE_INFINITY);
    public static final FConstant NAN = new FConstant<>(FSpecialVal.NOT_A_NUMBER, INDEX_OF_NAN);

    private static final Map<Long, FConstant> SPECIAL_CONST_MAP = FTiUtils.mapOf(
            INDEX_OF_NULL, NULL,
            INDEX_OF_POSITIVE_INFINITY, POSITIVE_INFINITY,
            INDEX_OF_NEGATIVE_INFINITY, NEGATIVE_INFINITY,
            INDEX_OF_NAN, NAN);

    /**
     * not null
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

    @SuppressWarnings("unchecked")
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

    public boolean isSpecialValue() {
        return SPECIAL_CONST_MAP.containsKey(index);
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

    public String toUserString(int maxDecimalPlace, boolean allowExponentialForm) {
        if (constant instanceof Float || constant instanceof Double) {
            final String round = FTiUtils.round(((Number) constant).doubleValue(), maxDecimalPlace, allowExponentialForm);
            return "'" + round + "'";
        } else {
            return toUserString();
        }
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
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FConstant<?> fConstant = (FConstant<?>) o;
        return index == fConstant.index &&
                Objects.equals(constant, fConstant.constant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(constant, index);
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

    private enum FSpecialVal implements Serializable {
        NULL("null"), POSITIVE_INFINITY("Inf"),
        NEGATIVE_INFINITY("-Inf"), NOT_A_NUMBER("NaN");

        private final String strVal;

        FSpecialVal(final String strVal) {
            this.strVal = strVal;
        }

        public String toString() {
            return strVal;
        }
    }
}
