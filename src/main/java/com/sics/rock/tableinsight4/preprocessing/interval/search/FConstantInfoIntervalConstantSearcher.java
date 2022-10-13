package com.sics.rock.tableinsight4.preprocessing.interval.search;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.preprocessing.constant.FConstant;
import com.sics.rock.tableinsight4.preprocessing.interval.FInterval;
import com.sics.rock.tableinsight4.preprocessing.interval.FIntervalConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.util.*;

/**
 * An interval constant searcher using constant-info in columns as interval constants
 *
 * @author zhaorx
 */
public class FConstantInfoIntervalConstantSearcher implements FIIntervalConstantSearcher {

    private final Set<FOperator> operators = new HashSet<>();

    @Override
    public List<FIntervalConstantInfo> search(final FTableDatasetMap tableDatasetMap) {
        if (operators.isEmpty()) return Collections.emptyList();

        final List<FIntervalConstantInfo> ret = new ArrayList<>();

        tableDatasetMap.allTableInfos().forEach(tableInfo -> {
            final String tableName = tableInfo.getTableName();
            tableInfo.intervalRequiredColumnsView().forEach(column -> {
                final String columnName = column.getColumnName();
                final FValueType valueType = column.getValueType();

                for (final FConstant<?> constant : column.getConstants()) {
                    if (constant.isSpecialValue()) continue;
                    final Object constValue = constant.getConstant();

                    final List<FInterval> intervals = new ArrayList<>();
                    for (final FOperator operator : operators) {
                        final List<FInterval> intervalOne = FInterval.of(operator.symbol + " " + constValue, valueType);
                        FAssertUtils.require(intervalOne.size() == 1,
                                () -> "Parse " + operator + " " + constValue + ", but get " + intervalOne);
                        intervals.add(intervalOne.get(0));
                    }
                    ret.add(FIntervalConstantInfo.constantIntervalConstant(tableName, columnName, intervals));
                }
            });
        });

        return ret;
    }

    /**
     * split operators
     *
     * @param operators string value like ">=,<="
     */
    public FConstantInfoIntervalConstantSearcher(final String operators) {
        for (final String operator : operators.split(",")) {
            final FOperator op = FOperator.of(operator);
            this.operators.add(op);
        }
    }
}
