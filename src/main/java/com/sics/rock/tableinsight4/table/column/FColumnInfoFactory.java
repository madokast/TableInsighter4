package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;

/**
 * @author zhaorx
 */
public class FColumnInfoFactory {

    public static FColumnInfo createIdColumn(String idColName) {
        FColumnInfo id = new FColumnInfo(idColName, FValueType.LONG);
        id.setColumnType(FColumnType.ID);
        id.setFindConstant(false);
        id.setNullConstant(false);
        id.setRangeConstantInfo(FValueRangeConfig.notFind());
        id.setSkip(true);
        id.setTarget(false);
        return id;
    }

    public static FColumnInfo createExternalDerivedColumn(
            String externalDerivedColumnName, FExternalBinaryModelInfo externalBinaryModelInfo) {
        FColumnInfo externalDerivedColumn = new FColumnInfo(externalDerivedColumnName, FValueType.LONG);
        externalDerivedColumn.setColumnType(FColumnType.EXTERNAL_BINARY_MODEL);
        externalDerivedColumn.setFindConstant(false);
        externalDerivedColumn.setNullConstant(false);
        externalDerivedColumn.setRangeConstantInfo(FValueRangeConfig.notFind());
        externalDerivedColumn.setSkip(false);
        externalDerivedColumn.setTarget(externalBinaryModelInfo.isTarget());
        return externalDerivedColumn;
    }
}
