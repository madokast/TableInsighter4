package com.sics.rock.tableinsight4.table;

public final class FTables {

    public static String columnIdentifier(String tableName, String columnName) {
        return tableName + "." + columnName;
    }


    private FTables() {
    }
}
