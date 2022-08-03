package com.sics.rock.tableinsight4.predicate.factory;

import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.impl.*;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnType;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * predicate factory
 *
 * @author zhaorx
 */
public class FPredicateFactory {

    private static final Logger logger = LoggerFactory.getLogger(FPredicateFactory.class);

    /**
     * create single-line rules like t0.name = aaa
     */
    public static FPredicateIndexer createSingleLinePredicateFactory(
            FTableInfo table, FDerivedColumnNameHandler derivedColumnNameHandler, List<FExternalPredicateInfo> otherInfos) {
        final FPredicateIndexer predicateIndexer = new FPredicateIndexer();

        final String tabName = table.getTableName();
        final String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
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

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);

        return predicateIndexer;
    }

    public static FPredicateIndexer createSingleTableCrossLinePredicateFactory(
            FTableInfo table, boolean constantPredicate, FDerivedColumnNameHandler derivedColumnNameHandler,
            List<FExternalPredicateInfo> otherInfos) {
        FPredicateIndexer predicateIndexer = new FPredicateIndexer();
        String tabName = table.getTableName();
        String innerTableName = table.getInnerTableName();

        table.nonSkipColumnsView().forEach(columnInfo -> {
            final String columnName = columnInfo.getColumnName();
            // identifier
            final Set<String> innerTabCols = derivedColumnNameHandler.innerTabCols(tabName, innerTableName, columnName);

            if (constantPredicate) {
                columnInfo.getConstants().stream().flatMap(cons -> {
                    FIPredicate t0 = new FUnaryConsPredicate(tabName, columnName, 0, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t1 = new FUnaryConsPredicate(tabName, columnName, 1, FOperator.EQ, cons, innerTabCols);
                    FIPredicate t01 = new FBinaryConsPredicate(tabName, columnName, FOperator.EQ, cons, innerTabCols);
                    return Stream.of(t0, t1, t01);
                }).forEach(predicateIndexer::put);
                columnInfo.getIntervalConstants().stream().flatMap(interval -> {
                    FIPredicate t0 = new FUnaryIntervalConsPredicate(tabName, columnName, 0, interval, innerTabCols);
                    FIPredicate t1 = new FUnaryIntervalConsPredicate(tabName, columnName, 1, interval, innerTabCols);
                    FIPredicate t01 = new FBinaryIntervalConsPredicate(tabName, columnName, interval, innerTabCols);
                    return Stream.of(t0, t1, t01);
                }).forEach(predicateIndexer::put);
            }

            if (columnInfo.getColumnType().equals(FColumnType.NORMAL)) {
                predicateIndexer.put(new FBinaryPredicate(tabName, tabName, columnName, columnName, FOperator.EQ, innerTabCols));
            }

            if (columnInfo.getColumnType().equals(FColumnType.EXTERNAL_BINARY_MODEL)) {

                predicateIndexer.put(new FBinaryModelPredicate(tabName, tabName, columnName, innerTabCols,
                        derivedColumnNameHandler.extractModelInfo(columnName)
                                .getPredicateNameFormatter().format(0, 1))
                );
            }

        });

        otherInfos.stream().map(FExternalPredicateInfo::predicates).flatMap(List::stream).forEach(predicateIndexer::put);


        return predicateIndexer;
    }


}
