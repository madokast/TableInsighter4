package com.sics.rock.tableinsight4.table;

import com.sics.rock.tableinsight4.core.constant.FConstant;
import com.sics.rock.tableinsight4.core.interval.FInterval;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import com.sics.rock.tableinsight4.table.column.FConstantConfig;
import com.sics.rock.tableinsight4.table.column.FIntervalConstantConfig;
import com.sics.rock.tableinsight4.table.column.FValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Column type
     */
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
     * Relative config info especially for each column
     * if there configs are null, back to upper config
     */
    private FConstantConfig constantConfig = FConstantConfig.findUsingDefault();

    /**
     * Does this column generate interval-constant predicate
     * Relative config info especially for each column
     * if there configs are null, back to upper config
     */
    private FIntervalConstantConfig intervalConstantInfo = FIntervalConstantConfig.notFindIntervalConstant();

    /**
     * Constants found in the column
     */
    private final ArrayList<FConstant<?>> constants = new ArrayList<>();

    /**
     * interval-constant found in the column
     */
    private final ArrayList<FInterval> intervalConstants = new ArrayList<>();

    public FColumnInfo(String columnName, FValueType valueType) {
        this.columnName = columnName;
        this.valueType = valueType;
    }

    public void addConstant(FConstant<?> constant) {
        this.constants.add(constant);
    }

    public void addIntervalConstant(FInterval interval) {
        this.intervalConstants.add(interval);
    }

    public void addIntervalConstants(List<FInterval> intervals) {
        this.intervalConstants.addAll(intervals);
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

    public FConstantConfig getConstantConfig() {
        return constantConfig;
    }

    public void setConstantConfig(FConstantConfig constantConfig) {
        this.constantConfig = constantConfig;
    }

    public FIntervalConstantConfig getIntervalConstantInfo() {
        return intervalConstantInfo;
    }

    public void setIntervalConstantInfo(FIntervalConstantConfig intervalConstantInfo) {
        this.intervalConstantInfo = intervalConstantInfo;
    }

    public ArrayList<FConstant<?>> getConstants() {
        return constants;
    }

    public ArrayList<FInterval> getIntervalConstants() {
        return intervalConstants;
    }
}