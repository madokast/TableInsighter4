package com.sics.rock.tableinsight4.procedure.constant.search;

import com.sics.rock.tableinsight4.procedure.constant.FConstant;
import com.sics.rock.tableinsight4.procedure.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.column.FValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FExternalConstantSearcher implements FIConstantSearcher {

    private static final Logger logger = LoggerFactory.getLogger(FExternalConstantSearcher.class);

    @Override
    public List<FConstantInfo> search(FTableDatasetMap tableDatasetMap) {
        final List<FConstantInfo> ret = new ArrayList<>();
        tableDatasetMap.foreach((tabInfo, data) -> {
            final String tableName = tabInfo.getTableName();
            final ArrayList<FColumnInfo> columns = tabInfo.getColumns();
            for (FColumnInfo column : columns) {
                final String columnName = column.getColumnName();
                final FValueType valueType = column.getValueType();
                final ArrayList<String> constants = column.getConstantConfig().getExternalConstants();
                constants.stream()
                        .map(valueType::cast).distinct()
                        .map(FConstant::new)
                        .map(cons -> new FConstantInfo(tableName, columnName, valueType, cons))
                        .forEach(ret::add);
            }
        });
        return ret;
    }
}
