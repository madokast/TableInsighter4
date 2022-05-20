package com.sics.rock.tableinsight4.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Meta info of a table column
 *
 * @author zhaorx
 */
public class FColumnInfo implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FColumnInfo.class);

    private final String columnName;

    private final FColumnType columnType;

    /**
     * Type of values in the column
     * Str、int、double ...
     */
    private Class<?> valueType;

    /**
     * When skip, the column is not used in rule mining
     */
    private boolean skip = false;

    /**
     * When target is false, the column is not used in the right of a rule
     */
    private boolean target = true;

    /**
     * Does this column generate constant predicate
     */
    private boolean findConstant = true;

    /**
     * Does this column generate range-constant predicate
     */
    private boolean rangeConstant = true;

    /**
     * Does this column generate null-constant predicate
     */
    private boolean nullConstant = true;

    /**
     * Constants found in the column
     */
    private ArrayList<FConstant<?>> constants = new ArrayList<>();

    public FColumnInfo(String columnName, FColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public void addConstant(FConstant<?> constant) {
        this.constants.add(constant);
    }

    // ----------------- getter setter -----------------------

    public String getColumnName() {
        return columnName;
    }

    public FColumnType getColumnType() {
        return columnType;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    public void setValueType(Class<?> valueType) {
        this.valueType = valueType;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public boolean isTarget() {
        return target;
    }

    public void setTarget(boolean target) {
        this.target = target;
    }

    public boolean isFindConstant() {
        return findConstant;
    }

    public void setFindConstant(boolean findConstant) {
        this.findConstant = findConstant;
    }

    public boolean isRangeConstant() {
        return rangeConstant;
    }

    public void setRangeConstant(boolean rangeConstant) {
        this.rangeConstant = rangeConstant;
    }

    public boolean isNullConstant() {
        return nullConstant;
    }

    public void setNullConstant(boolean nullConstant) {
        this.nullConstant = nullConstant;
    }

    public ArrayList<FConstant<?>> getConstants() {
        return constants;
    }

    public void setConstants(ArrayList<FConstant<?>> constants) {
        this.constants = constants;
    }
}