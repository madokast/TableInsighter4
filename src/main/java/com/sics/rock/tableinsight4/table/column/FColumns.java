package com.sics.rock.tableinsight4.table.column;

public final class FColumns {

    public static String columnIdentifier(String tableName, String columnName) {
        return tableName + "." + columnName;
    }

    private FColumns() {
    }
}
