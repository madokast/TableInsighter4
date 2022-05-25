package com.sics.rock.tableinsight4.table;

/**
 * @author zhaorx
 */
public class FColumnInfoFactory {


    public static FColumnInfo createIdColumn(String idColName) {
        FColumnInfo id = new FColumnInfo(idColName, Long.class);
        id.setColumnType(FColumnType.ID);
        id.setFindConstant(false);
        id.setNullConstant(false);
        id.setRangeConstant(false);
        id.setSkip(true);
        id.setTarget(false);
        return id;
    }

}
