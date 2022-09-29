package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;

/**
 * column info factory
 *
 * @author zhaorx
 */
public class FColumnInfoFactory {

    public static FColumnInfo createIdColumn(String idColName) {
        FColumnInfo id = new FColumnInfo(idColName, FValueType.LONG);
        id.setColumnType(FColumnType.ID);
        id.setConstantConfig(FConstantConfig.notFindConstantValue());
        id.setIntervalConstantInfo(FIntervalConstantConfig.notFindIntervalConstant());
        id.setSkip(true);
        id.setTarget(false);
        return id;
    }

    public static FColumnInfo createExternalDerivedColumn(
            String externalDerivedColumnName, FExternalBinaryModelInfo externalBinaryModelInfo) {
        FColumnInfo externalDerivedColumn = new FColumnInfo(externalDerivedColumnName, FValueType.LONG);
        externalDerivedColumn.setColumnType(FColumnType.EXTERNAL_BINARY_MODEL);
        final FConstantConfig constantConfig = FConstantConfig.notFindConstantValue();
        externalDerivedColumn.setConstantConfig(constantConfig);
        externalDerivedColumn.setIntervalConstantInfo(FIntervalConstantConfig.notFindIntervalConstant());
        externalDerivedColumn.setSkip(false);
        externalDerivedColumn.setTarget(externalBinaryModelInfo.isTarget());
        return externalDerivedColumn;
    }
}
