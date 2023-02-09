package com.sics.rock.tableinsight4.predicate.factory.impl;

import com.sics.rock.tableinsight4.pli.FPLI;
import com.sics.rock.tableinsight4.predicate.FColumnComparator;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FIPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryModelPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FBinaryPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * create binary-table cross-line (2-line) predicates like tab1.t0.name = tab2.t1.name
 * in default:
 * the binary (2-line) predicates
 * are created
 *
 * @author zhaorx
 */
public class FBinaryTableCrossLinePredicateFactory implements FIPredicateFactory, Serializable {

    @Override
    public FPredicateIndexer createPredicates() {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();
        final String leftTableName = leftTable.getTableName();
        final String rightTableName = rightTable.getTableName();
        final String leftInnerTableName = leftTable.getInnerTableName();
        final String rightInnerTableName = rightTable.getInnerTableName();

        leftTable.nonSkipColumnsView().forEach(leftColumnInfo -> {
            final String leftColumnName = leftColumnInfo.getColumnName();
            final FValueType leftValueType = leftColumnInfo.getValueType();
            final Supplier<Set<String>> leftInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(leftTableName, leftInnerTableName, leftColumnName);

            rightTable.nonSkipColumnsView().forEach(rightColumnInfo -> {
                // value type should be identical
                if (!leftValueType.equals(rightColumnInfo.getValueType())) return;

                final String rightColumnName = rightColumnInfo.getColumnName();
                final Supplier<Set<String>> rightInnerTabCols = () -> derivedColumnNameHandler.innerTabCols(rightTableName, rightInnerTableName, rightColumnName);
                final Supplier<Set<String>> innerTabCols = () -> FTiUtils.setUnion(leftInnerTabCols.get(), rightInnerTabCols.get()); // identifier

                // model derived column
                if (derivedColumnNameHandler.isDerivedBinaryModelColumnName(leftColumnName)) {
                    if (!leftColumnName.equals(rightColumnName)) return;
                    final String leftTable_ = derivedColumnNameHandler.extractModelInfo(leftColumnName).getLeftTableName();
                    final String rightTable_ = derivedColumnNameHandler.extractModelInfo(rightColumnName).getRightTableName();
                    if (FTiUtils.setOf(leftTableName, rightTableName).equals(FTiUtils.setOf(leftTable_, rightTable_))) {
                        predicateIndexer.put(new FBinaryModelPredicate(leftTableName, rightTableName, leftColumnName, innerTabCols.get()));
                    }
                }
                // normal column. check similarity
                else if (leftValueType.equals(FValueType.STRING) && columnSimilarity.columnSimilarity(leftColumnInfo, rightColumnInfo) >= crossColumnThreshold) {
                    predicateIndexer.put(new FBinaryPredicate(leftTableName, rightTableName, leftColumnName, rightColumnName, FOperator.EQ, innerTabCols.get()));
                } else if (leftValueType.isComparable() && columnSimilarity.columnComparable(leftColumnInfo, rightColumnInfo)) {
                    predicateIndexer.put(new FBinaryPredicate(leftTableName, rightTableName, leftColumnName, rightColumnName, FOperator.EQ, innerTabCols.get()));
                    for (final FOperator operator : comparableColumnOperators) {
                        predicateIndexer.put(new FBinaryPredicate(leftTableName, rightTableName, leftColumnName, rightColumnName, operator, innerTabCols.get()));
                    }
                }

            }); // end right loop
        }); // end left loop

        // constant predicate
        createConstantPredicates(predicateIndexer, leftTableName, leftInnerTableName, leftTable);
        createConstantPredicates(predicateIndexer, rightTableName, rightInnerTableName, rightTable);
        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        return predicateIndexer;
    }

    private void createConstantPredicates(final FPredicateIndexer predicateIndexer, final String tableName, final String innerTableName, final FTableInfo rightTable) {
        rightTable.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tableName, innerTableName, columnName);

            columnInfo.getConstants().forEach(cons -> {
                FIPredicate t0 = new FUnaryConsPredicate(tableName, columnName, 0, FOperator.EQ, cons, innerTabCols);
                FIPredicate t1 = new FUnaryConsPredicate(tableName, columnName, 1, FOperator.EQ, cons, innerTabCols);

                // order matters!!
                predicateIndexer.put(t0);
                predicateIndexer.put(t1);

                predicateIndexer.insertConstPredT01Map(t0, t1);

            });
            columnInfo.getIntervalConstants().forEach(interval -> {
                FIPredicate t0 = new FUnaryIntervalConsPredicate(tableName, columnName, 0, interval, innerTabCols);
                FIPredicate t1 = new FUnaryIntervalConsPredicate(tableName, columnName, 1, interval, innerTabCols);

                predicateIndexer.put(t0);
                predicateIndexer.put(t1);

                predicateIndexer.insertConstPredT01Map(t0, t1);
            });
        });
    }

    /*===================== materials =====================*/

    private final FTableInfo leftTable;

    private final FTableInfo rightTable;

    private final List<FExternalPredicateInfo> otherInfos;


    /*===================== configs & infos =====================*/

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    private final double crossColumnThreshold;

    private final List<FOperator> comparableColumnOperators;

    private final FColumnComparator columnSimilarity;


    public FBinaryTableCrossLinePredicateFactory(
            final FTableInfo leftTable, final FTableInfo rightTable,
            final List<FExternalPredicateInfo> otherInfos,
            final FDerivedColumnNameHandler derivedColumnNameHandler, final FTableDatasetMap datasetMap,
            final FPLI PLI, final double crossColumnThreshold,
            final String comparableColumnOperators) {
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.otherInfos = otherInfos;
        this.derivedColumnNameHandler = derivedColumnNameHandler;
        this.columnSimilarity = new FColumnComparator(PLI, datasetMap, leftTable, rightTable);
        this.crossColumnThreshold = crossColumnThreshold;
        this.comparableColumnOperators = Arrays.stream(comparableColumnOperators.split(","))
                .filter(StringUtils::isNotBlank).map(String::trim).map(FOperator::of)
                .filter(op -> !op.equals(FOperator.EQ))
                .distinct().collect(Collectors.toList());
    }
}
