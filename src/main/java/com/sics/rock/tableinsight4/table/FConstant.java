package com.sics.rock.tableinsight4.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * A container holds constant.
 *
 * @param <T> constant type
 * @author zhaorx
 */
public class FConstant<T> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FTableInfo.class);

    /**
     * Nullable
     */
    private final T constant;

    private final long occurrence;

    private final long index;

    public FConstant(T constant, long occurrence, long index) {
        this.constant = constant;
        this.occurrence = occurrence;
        this.index = index;
    }

    @Override
    public String toString() {
        return constant + "[" + occurrence + "(occ)]";
    }

    public static Logger getLogger() {
        return logger;
    }

    public T getConstant() {
        return constant;
    }

    public long getOccurrence() {
        return occurrence;
    }

    public long getIndex() {
        return index;
    }
}
