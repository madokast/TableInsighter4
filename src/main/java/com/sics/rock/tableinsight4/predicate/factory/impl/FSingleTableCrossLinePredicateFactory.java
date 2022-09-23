package com.sics.rock.tableinsight4.predicate.factory.impl;

import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FIPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * create single-table cross-line (2-line) predicates like t0.name = t1.name
 * in default:
 * the unary (single-line) constant predicates
 * the unary (single-line) interval constant predicates
 * the binary (2-line) predicates
 * are created
 *
 * @author zhaorx
 */
public class FSingleTableCrossLinePredicateFactory implements FIPredicateFactory {


    @Override
    public FPredicateIndexer createPredicates() {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();
        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName, columnName);

            if (constantPredicateFlag) {
                columnInfo.getConstants().forEach(cons -> {
                    FIPredicate t0 = new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t1 = new FUnaryConsPredicate(tabName, columnName, 1, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t01 = new FBinaryConsPredicate(tabName, columnName, FOperator.EQ, cons, innerTabCols);

                    // order matters!!
                    predicateIndexer.put(t0);
                    predicateIndexer.put(t1);
                    predicateIndexer.put(t01);

                    predicateIndexer.insertConstPredT01Map(t0, t1);
                    predicateIndexer.insertConstBiPred2DivisionPred(t01, t0, t1);

                });
                columnInfo.getIntervalConstants().forEach(interval -> {
                    FIPredicate t0 = new FUnaryIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols);
                    FIPredicate t1 = new FUnaryIntervalConsPredicate(tabName, columnName, 1, interval, innerTabCols);
                    FIPredicate t01 = new FBinaryIntervalConsPredicate(tabName, columnName, interval, innerTabCols);

                    predicateIndexer.put(t0);
                    predicateIndexer.put(t1);
                    predicateIndexer.put(t01);

                    predicateIndexer.insertConstPredT01Map(t0, t1);
                    predicateIndexer.insertConstBiPred2DivisionPred(t01, t0, t1);
                });
            }

            if (columnInfo.getColumnType().equals(FColumnType.NORMAL)) {
                predicateIndexer.put(new FBinaryPredicate(tabName, tabName, columnName, columnName, FOperator.EQ, innerTabCols));
                if (columnInfo.getValueType().isComparable()) {
                    for (final FOperator op : comparableColumnOperators) {
                        predicateIndexer.put(new FBinaryPredicate(tabName, tabName, columnName, columnName, op, innerTabCols));
                    }
                }
            }

            if (columnInfo.getColumnType().equals(FColumnType.EXTERNAL_BINARY_MODEL)) {
                predicateIndexer.put(new FBinaryModelPredicate(tabName, tabName, columnName, innerTabCols));
            }

        });

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);


        return predicateIndexer;
    }


    /*===================== materials =====================*/

    private final FTableInfo table;

    private final List<FExternalPredicateInfo> otherInfos;


    /*===================== configs =====================*/

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    private final boolean constantPredicateFlag;

    private final List<FOperator> comparableColumnOperators;

    /**
     * @param comparableColumnOperators like ">=,<="
     */
    public FSingleTableCrossLinePredicateFactory(
            final FTableInfo table, final List<FExternalPredicateInfo> otherInfos,
            final FDerivedColumnNameHandler derivedColumnNameHandler, final boolean constantPredicateFlag,
            final String comparableColumnOperators) {
        this.table = table;
        this.otherInfos = otherInfos;
        this.derivedColumnNameHandler = derivedColumnNameHandler;
        this.constantPredicateFlag = constantPredicateFlag;
        this.comparableColumnOperators = Arrays.stream(comparableColumnOperators.split(","))
                .filter(StringUtils::isNotBlank).map(String::trim).map(FOperator::of)
                .filter(op -> !op.equals(FOperator.EQ))
                .distinct().collect(Collectors.toList());
    }
}
