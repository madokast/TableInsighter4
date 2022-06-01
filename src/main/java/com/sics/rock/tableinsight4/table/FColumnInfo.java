package com.sics.rock.tableinsight4.table;

import com.sics.rock.tableinsight4.core.constant.FConstant;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import com.sics.rock.tableinsight4.table.column.FValueRangeConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
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

    /**
     * Type of values in the column
     */
    private final FValueType valueType;

    private FColumnType columnType = FColumnType.NORMAL;

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
    private FValueRangeConfig rangeConstantInfo = FValueRangeConfig.notFind();

    /**
     * Does this column generate null-constant predicate
     */
    private boolean nullConstant = true;

    /**
     * Constants found in the column
     */
    private ArrayList<FConstant<?>> constants = new ArrayList<>();

    public FColumnInfo(String columnName, FValueType valueType) {
        this.columnName = columnName;
        this.valueType = valueType;
    }

    public void addConstant(FConstant<?> constant) {
        this.constants.add(constant);
    }

    // ----------------- getter setter -----------------------


    public String getColumnName() {
        return columnName;
    }

    public FValueType getValueType() {
        return valueType;
    }

    public FColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(FColumnType columnType) {
        this.columnType = columnType;
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

    public FValueRangeConfig getRangeConstantInfo() {
        return rangeConstantInfo;
    }

    public void setRangeConstantInfo(FValueRangeConfig rangeConstantInfo) {
        this.rangeConstantInfo = rangeConstantInfo;
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