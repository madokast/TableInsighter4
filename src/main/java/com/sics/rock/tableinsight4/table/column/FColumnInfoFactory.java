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
        final FConstantConfig constantConfig = FConstantConfig.findConstantValue();
        constantConfig.config(FConstantConfig.CONFIG_DOWN_LIMIT_RATIO, 0.0);
        constantConfig.config(FConstantConfig.CONFIG_UPPER_LIMIT_RATIO, 1.0);
        externalDerivedColumn.setConstantConfig(constantConfig);
        externalDerivedColumn.setIntervalConstantInfo(FIntervalConstantConfig.notFindIntervalConstant());
        externalDerivedColumn.setSkip(false);
        externalDerivedColumn.setTarget(externalBinaryModelInfo.isTarget());
        return externalDerivedColumn;
    }
}
