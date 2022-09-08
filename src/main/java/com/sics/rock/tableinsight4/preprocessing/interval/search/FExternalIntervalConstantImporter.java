package com.sics.rock.tableinsight4.preprocessing.interval.search;

import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.column.FValueType;

import java.util.ArrayList;
import java.util.List;

/**
 * Convert externalIntervalConstant:str to FIntervalConstantInfo
 * The externalIntervalConstant is a string depicting interval constant in its column
 * <p>
 * The formats of externalIntervalConstant are listed below
 * 1. "a number". Create intervals (-Inf, num] and (num, +inf)
 * 2. "op number". Like ">5", ">=10", "≤20"
 * 3. "interval". Like "[3,5]", "(12,30]"
 */
public class FExternalIntervalConstantImporter implements FIIntervalConstantSearcher {

    @Override
    public List<FIntervalConstantInfo> search(FTableDatasetMap tableDatasetMap) {
        final List<FIntervalConstantInfo> ret = new ArrayList<>();
        tableDatasetMap.foreach((tabInfo, data) -> {
            final String tableName = tabInfo.getTableName();
            final ArrayList<FColumnInfo> columns = tabInfo.getColumns();
            for (FColumnInfo column : columns) {
                final String columnName = column.getColumnName();
                FValueType valueType = column.getValueType();
                final ArrayList<String> externalIntervalConstants = column.getIntervalConstantInfo().getExternalIntervalConstants();
                for (String externalIntervalConstant : externalIntervalConstants) {
                    final List<FInterval> intervalList = FInterval.of(externalIntervalConstant, valueType);
                    ret.add(FIntervalConstantInfo.externalColumnIntervalConstant(tableName, columnName, intervalList));
                }
            }
        });
        return ret;
    }
}
