package com.sics.rock.tableinsight4.table;

import com.sics.rock.tableinsight4.procedure.external.FExternalBinaryModelInfo;

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

    public static FColumnInfo createExternalDerivedColumn(
            String externalDerivedColumnName, FExternalBinaryModelInfo externalBinaryModelInfo) {
        FColumnInfo externalDerivedColumn = new FColumnInfo(externalDerivedColumnName, Long.class);
        externalDerivedColumn.setColumnType(FColumnType.EXTERNAL_BINARY_MODEL);
        externalDerivedColumn.setFindConstant(false);
        externalDerivedColumn.setNullConstant(false);
        externalDerivedColumn.setRangeConstant(false);
        externalDerivedColumn.setSkip(false);
        externalDerivedColumn.setTarget(externalBinaryModelInfo.isTarget());
        return externalDerivedColumn;
    }
}
