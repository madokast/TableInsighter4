package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.io.Serializable;

/**
 * typedef String ColumnName
 * for readability only
 *
 * @author zhaorx
 */
public class FColumnName implements Serializable {

    public final String columnName;

    public FColumnName(String columnName) {
        FAssertUtils.require(columnName != null, "ColumnName cannot be null");
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FColumnName that = (FColumnName) o;
        return columnName.equals(that.columnName);
    }

    @Override
    public int hashCode() {
        return columnName.hashCode();
    }

    @Override
    public String toString() {
        return columnName;
    }
}
