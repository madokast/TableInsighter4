package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.core.external.FExternalBinaryModelInfo;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.utils.FAssertUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FDerivedColumnNameHandler implements FTiEnvironment {

    private final String combineColumnLinker = config().combineColumnLinker;
    private final String combineColumnLinkerRegex = config().combineColumnLinkerRegex;

    public String deriveCombinedColumn(List<String> combinedColumnNames) {
        return String.join(combineColumnLinker, combinedColumnNames);
    }

    public List<String> parseCombinedColumn(String combinedColumnName) {
        FAssertUtils.require(() -> combinedColumnName.contains(combineColumnLinker),
                () -> combinedColumnName + " is not a derived combined column name.");
        return Arrays.asList(combinedColumnName.split(combineColumnLinkerRegex));
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


    public FDerivedColumnNameHandler(List<FExternalBinaryModelInfo> externalBinaryModelInfos) {
        cachedModelInfoMap = externalBinaryModelInfos.stream()
                .collect(Collectors.toMap(FExternalBinaryModelInfo::getId, Function.identity()));
    }
}
