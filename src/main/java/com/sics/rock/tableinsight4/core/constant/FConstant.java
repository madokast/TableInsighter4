package com.sics.rock.tableinsight4.core.constant;

import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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

    /**
     * Nullable
     */
    private final T constant;

    /**
     * lazy init
     * -3: not init
     * -2: not found
     * -1: null
     * >=0 : index for notnull value
     */
    private long index = INDEX_NOT_INIT;

    public FConstant(T constant) {
        this.constant = constant;
    }

    @Override
    public String toString() {
        return Objects.toString(constant);
    }

    public T getConstant() {
        return constant;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getIndex() {
        FAssertUtils.require(() -> index >= -1, "The index of constant has not be set");
        return index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FConstant<?> fConstant = (FConstant<?>) o;
        return Objects.equals(constant, fConstant.constant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(constant);
    }
}
