package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.io.Serializable;
import java.util.Arrays;

/**
 * typedef String ColumnName
 * for readability only
 *
 * @author zhaorx
 */
public class FColumnName implements Serializable {

    public final char[] columnName;

    private final int hashCode;

    public FColumnName(String columnName) {
        FAssertUtils.require(columnName != null, "ColumnName cannot be null");
        this.columnName = columnName.toCharArray();
        this.hashCode = columnName.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FColumnName that = (FColumnName) o;
        return hashCode == that.hashCode && Arrays.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return String.valueOf(columnName);
    }
}
