package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.predicate.FIPredicate;
import com.sics.rock.tableinsight4.preprocessing.external.binary.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FDerivedColumnNameHandler implements FTiEnvironment {

    private final String combineColumnLinker = config().combineColumnLinker;
    private final String combineColumnLinkerRegex = config().combineColumnLinkerRegex;
    private final String tableColumnLinker = config().tableColumnLinker;

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

    private final String externalBinaryModelDerivedColumnSuffix = config().externalBinaryModelDerivedColumnSuffix;
    private final Map<String, FExternalBinaryModelInfo> cachedModelInfoMap;

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

    /**
     * identifier of predicate
     * @see FIPredicate#innerTabCols()
     */
    public Set<String> innerTabCols(String tabName, String innerTableName, String columnName) {
        if(isDerivedBinaryModelColumnName(columnName)) {
            FExternalBinaryModelInfo modelInfo = extractModelInfo(columnName);
            if (modelInfo.getLeftTableName().equals(tabName)) {
                return modelInfo.getLeftColumns().stream().map(c -> innerTableName + tableColumnLinker + c).collect(Collectors.toSet());
            } else if (modelInfo.getRightTableName().equals(tabName)) {
                return modelInfo.getRightColumns().stream().map(c -> innerTableName + tableColumnLinker + c).collect(Collectors.toSet());
            } else {
                throw new IllegalArgumentException(columnName + " is a derived model column but not belong to model " + modelInfo);
            }
        }

        return tryParseCombinedColumn(columnName).stream().map(c -> innerTableName + tableColumnLinker + c).collect(Collectors.toSet());
    }
}
