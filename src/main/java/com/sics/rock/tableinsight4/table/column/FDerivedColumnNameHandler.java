package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * derived column name handler
 * corresponding column name <==> derived column name
 * <p>
 * 1. combined column
 * 2. model column
 *
 * @author zhaorx
 */
public class FDerivedColumnNameHandler implements FTiEnvironment {

    private final String combineColumnLinker = config().combineColumnLinker;
    private final String combineColumnLinkerRegex = config().combineColumnLinkerRegex;
    private final String tableColumnLinker = config().tableColumnLinker;
    private final String externalBinaryModelDerivedColumnSuffix = config().externalBinaryModelDerivedColumnSuffix;
    // model-id -> info
    private final Map<String, FExternalBinaryModelInfo> cachedModelInfoMap;

    /*============================ combined column handler ====================================*/

    public String deriveCombinedColumn(List<String> combinedColumnNames) {
        return String.join(combineColumnLinker, combinedColumnNames);
    }

    public List<String> parseCombinedColumn(String combinedColumnName) {
        FAssertUtils.require(() -> combinedColumnName.contains(combineColumnLinker),
                () -> combinedColumnName + " is not a derived combined column name.");
        return Arrays.asList(combinedColumnName.split(combineColumnLinkerRegex));
    }

    public boolean isCombinedColumnName(String maybeCombinedColumnName) {
        return maybeCombinedColumnName.contains(combineColumnLinker);
    }

    public List<String> tryParseCombinedColumn(String maybeCombinedColumnName) {
        if (isCombinedColumnName(maybeCombinedColumnName)) {
            return parseCombinedColumn(maybeCombinedColumnName);
        } else {
            return Collections.singletonList(maybeCombinedColumnName);
        }
    }

    /*============================ model column handler ====================================*/

    public String deriveModelColumn(String modelId) {
        FAssertUtils.require(() -> cachedModelInfoMap.containsKey(modelId),
                () -> "No model in " + cachedModelInfoMap + " matches " + modelId);
        return externalBinaryModelDerivedColumnSuffix + modelId;
    }

    public FExternalBinaryModelInfo extractModelInfo(String derivedModelColumnName) {
        FAssertUtils.require(() -> derivedModelColumnName.startsWith(externalBinaryModelDerivedColumnSuffix),
                () -> derivedModelColumnName + " is not a derived model column name.");
        String modelId = derivedModelColumnName.substring(externalBinaryModelDerivedColumnSuffix.length());
        FAssertUtils.require(() -> cachedModelInfoMap.containsKey(modelId),
                () -> "No model info " + cachedModelInfoMap + " matches id " + modelId);
        return cachedModelInfoMap.get(modelId);
    }

    public boolean isDerivedBinaryModelColumnName(String maybeDerivedModelColumnName) {
        if (maybeDerivedModelColumnName.startsWith(externalBinaryModelDerivedColumnSuffix)) {
            final String modelId = maybeDerivedModelColumnName.substring(externalBinaryModelDerivedColumnSuffix.length());
            return cachedModelInfoMap.containsKey(modelId);
        }
        return false;
    }


    public FDerivedColumnNameHandler(List<FExternalBinaryModelInfo> externalBinaryModelInfos) {
        cachedModelInfoMap = externalBinaryModelInfos.stream()
                .collect(Collectors.toMap(FExternalBinaryModelInfo::getId, Function.identity()));
    }

    /*============================ derived column handler ====================================*/

    /**
     * create a identifier/identifiers of the column
     *
     * bug-fix: 2022-10-11
     * Derived columns should be included either. The derived columns holds infos like Column::target
     * which used in RHSs filtering.
     *
     * @see FIPredicate#innerTabCols()
     */
    public Set<String> innerTabCols(String tabName, String innerTableName, String columnName) {
        final Set<String> identifiers = new HashSet<>();
        identifiers.add(innerTableName + tableColumnLinker + columnName);
        if (isDerivedBinaryModelColumnName(columnName)) {
            FExternalBinaryModelInfo modelInfo = extractModelInfo(columnName);
            if (modelInfo.getLeftTableName().equals(tabName)) {
                modelInfo.getLeftColumns().stream().map(c -> innerTableName + tableColumnLinker + c).forEach(identifiers::add);
            } else if (modelInfo.getRightTableName().equals(tabName)) {
                modelInfo.getRightColumns().stream().map(c -> innerTableName + tableColumnLinker + c).forEach(identifiers::add);
            } else {
                throw new IllegalArgumentException(columnName + " is a derived model column but not belong to model " + modelInfo);
            }
        } else {
            tryParseCombinedColumn(columnName).stream().map(c -> innerTableName + tableColumnLinker + c).forEach(identifiers::add);
        }

        return identifiers;
    }
}
