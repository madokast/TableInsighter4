package com.sics.rock.tableinsight4.predicate.factory.impl;

import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FColumnComparator;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FIPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryCrossColumnPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * create single-line predicate like t0.name = aaa
 * the unary (single-line) constant predicates and unary (single-line) interval constant predicates will be created in default
 *
 * @author zhaorx
 */
public class FSingleLinePredicateFactory implements FIPredicateFactory {

    @Override
    public FPredicateIndexer createPredicates() {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();

        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        final List<FColumnInfo> columns = table.nonSkipColumnsView();
        columns.forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName, columnName);
            columnInfo.getConstants().stream().map(cons ->
                    new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols)
            ).forEach(predicateIndexer::put);
            columnInfo.getIntervalConstants().stream().map(interval ->
                    new FUnaryIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols)
            ).forEach(predicateIndexer::put);
        });

        if (singleLineCrossColumn) {
            for (int i = 0; i < columns.size(); i++) {
                final FColumnInfo leftColumnInfo = columns.get(i);
                final String leftColumnName = leftColumnInfo.getColumnName();
                final FValueType leftValueType = leftColumnInfo.getValueType();
                final Supplier<Set<String>> leftInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(tabName, innerTableName, leftColumnName);

                for (int j = i + 1; j < columns.size(); j++) {
                    final FColumnInfo rightColumnInfo = columns.get(j);
                    final String rightColumnName = rightColumnInfo.getColumnName();
                    if (!leftValueType.equals(rightColumnInfo.getValueType())) continue;

                    final Supplier<Set<String>> rightInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(tabName, innerTableName, rightColumnName);
                    final Supplier<Set<String>> innerTabCols = () -> FTiUtils.setUnion(leftInnerTabCols.get(), rightInnerTabCols.get()); // identifier

                    if (leftValueType.equals(FValueType.STRING) && columnSimilarity.columnSimilarity(leftColumnInfo, rightColumnInfo) >= crossColumnThreshold) {
                        predicateIndexer.put(new FUnaryCrossColumnPredicate(tabName, leftColumnName, rightColumnName, 0, FOperator.EQ, innerTabCols.get()));
                    } else if (leftValueType.isComparable() && columnSimilarity.columnComparable(leftColumnInfo, rightColumnInfo)) {
                        predicateIndexer.put(new FUnaryCrossColumnPredicate(tabName, leftColumnName, rightColumnName, 0, FOperator.EQ, innerTabCols.get()));
                        for (final FOperator operator : comparableColumnOperators) {
                            predicateIndexer.put(new FUnaryCrossColumnPredicate(tabName, leftColumnName, rightColumnName, 0, operator, innerTabCols.get()));
                        }
                    }
                }
            }
        }

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        return predicateIndexer;
    }

    /*===================== materials =====================*/

    private final FTableInfo table;

    private final List<FExternalPredicateInfo> otherInfos;

    private final double crossColumnThreshold;

    private final boolean singleLineCrossColumn;

    private final List<FOperator> comparableColumnOperators;

    private final FColumnComparator columnSimilarity;


    /*===================== configs =====================*/

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    public FSingleLinePredicateFactory(final FTableInfo table, final List<FExternalPredicateInfo> otherInfos,
                                       final FDerivedColumnNameHandler derivedColumnNameHandler,
                                       final boolean singleLineCrossColumn, final double crossColumnThreshold,
                                       final String comparableColumnOperators,
                                       final FTableDatasetMap datasetMap, final FPLI PLI) {
        this.table = table;
        this.otherInfos = otherInfos;
        this.derivedColumnNameHandler = derivedColumnNameHandler;
        this.singleLineCrossColumn = singleLineCrossColumn;
        this.crossColumnThreshold = crossColumnThreshold;
        this.columnSimilarity = new FColumnComparator(PLI, datasetMap, table, table);
        this.comparableColumnOperators = Arrays.stream(comparableColumnOperators.split(","))
                .filter(StringUtils::isNotBlank).map(String::trim).map(FOperator::of)
                .filter(op -> !op.equals(FOperator.EQ))
                .distinct().collect(Collectors.toList());

    }
}
