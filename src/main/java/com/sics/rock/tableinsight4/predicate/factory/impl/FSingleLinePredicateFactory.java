package com.sics.rock.tableinsight4.predicate.factory.impl;

import com.sics.rock.tableinsight4.predicate.FOperator;
import com.sics.rock.tableinsight4.predicate.factory.FIPredicateFactory;
import com.sics.rock.tableinsight4.predicate.factory.FPredicateIndexer;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryConsPredicate;
import com.sics.rock.tableinsight4.predicate.impl.FUnaryIntervalConsPredicate;
import com.sics.rock.tableinsight4.predicate.info.FExternalPredicateInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FDerivedColumnNameHandler;

import java.util.List;
import java.util.Set;

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

    /*===================== materials =====================*/

    private final FTableInfo table;

    private final List<FExternalPredicateInfo> otherInfos;


    /*===================== configs =====================*/

    private final FDerivedColumnNameHandler derivedColumnNameHandler;

    public FSingleLinePredicateFactory(final FTableInfo table, final List<FExternalPredicateInfo> otherInfos,
                                       final FDerivedColumnNameHandler derivedColumnNameHandler) {
        this.table = table;
        this.otherInfos = otherInfos;
        this.derivedColumnNameHandler = derivedColumnNameHandler;
    }
}
