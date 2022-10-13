package com.sics.rock.tableinsight4.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Meta info of a table
 *
 * @author zhaorx
 */
public class FTableInfo implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FTableInfo.class);

    /**
     * User-defined table name.
     * For output and logging
     */
    private final String tableName;

    /**
     * Inner table name
     */
    private final String innerTableName;

    /**
     * Data path
     */
    private final String tableDataPath;

    /**
     * Length of the table
     * Lazy initialization and -1 as null
     */
    private long length = -1;

    /**
     * Columns of this table.
     * Including derived columns such as interval-column and model-column,
     * which are dynamic generated. As the result, the List is modifiable,
     * so we use ArrayList type
     */
    private ArrayList<FColumnInfo> columns = new ArrayList<>();

    public FTableInfo(String tableName, String innerTableName, String tableDataPath) {
        this.tableName = tableName;
        this.innerTableName = innerTableName;
        this.tableDataPath = tableDataPath;
    }

    public void addColumnInfo(FColumnInfo columnInfo) {
        this.columns.add(columnInfo);
    }

    /*---------------------- column view -----------------------*/

    public List<FColumnInfo> intervalRequiredColumnsView() {
        return columnsView(c -> !c.isSkip() && c.getIntervalConstantInfo().isFindIntervalConstant() && c.getValueType().isComparable());
    }

    public List<FColumnInfo> nonSkipColumnsView() {
        return columnsView(c -> !c.isSkip());
    }

    public List<FColumnInfo> nonTargetColumnsView() {
        return columnsView(c -> !c.isTarget());
    }

    private List<FColumnInfo> columnsView(Predicate<FColumnInfo> filter) {
        final List<FColumnInfo> view = columns.stream()
                .filter(filter)
                .collect(Collectors.toList());
        return Collections.unmodifiableList(view);
    }

    /**
     * @return a view of columnMap
     */
    public Map<String, FColumnInfo> columnMapView() {
        final Map<String, FColumnInfo> view = columns.stream()
                .collect(Collectors.toMap(FColumnInfo::getColumnName, Function.identity()));
        return Collections.unmodifiableMap(view);
    }

    /*---------------------- getter setter -----------------------*/

    public String getTableName() {
        return tableName;
    }

    public String getInnerTableName() {
        return innerTableName;
    }

    public String getTableDataPath() {
        return tableDataPath;
    }

    public long getLength(Supplier<Long> lengthSupplier) {
        if (length == -1) {
            length = lengthSupplier.get();
        }
        return length;
    }

    public ArrayList<FColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<FColumnInfo> columns) {
        this.columns = columns;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FTableInfo that = (FTableInfo) o;
        return innerTableName.equals(that.innerTableName);
    }

    @Override
    public int hashCode() {
        return innerTableName.hashCode();
    }

    @Override
    public String toString() {
        return tableName + "(" + innerTableName + ")";
    }
}
