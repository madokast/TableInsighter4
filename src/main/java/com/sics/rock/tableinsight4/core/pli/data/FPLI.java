package com.sics.rock.tableinsight4.core.pli.data;

import com.sics.rock.tableinsight4.internal.FPartitionId;
import com.sics.rock.tableinsight4.internal.FRddElementIndex;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FColumnName;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.HashMap;
import java.util.Map;

public class FPLI {

    /**
     * PLI
     */
    private final Map<FTableInfo, JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>>> PLI = new HashMap<>();

    /**
     * elementIndex -> rowId
     * for positive / negative examples of rules
     */
    private final Map<FTableInfo, JavaPairRDD<FRddElementIndex, Long>> IDColumnMap = new HashMap<>();

    public void putTablePLI(FTableInfo tableInfo, JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> pliOfTab) {
        PLI.put(tableInfo, pliOfTab);
    }

    public void putTableIDColumnMap(FTableInfo tableInfo, JavaPairRDD<FRddElementIndex, Long> IDMap) {
        IDColumnMap.put(tableInfo, IDMap);
    }

    public JavaPairRDD<FPartitionId, Map<FColumnName, FLocalPLI>> getTablePLI(FTableInfo tableInfo) {
        return PLI.get(tableInfo);
    }

    public JavaPairRDD<FRddElementIndex, Long> getTableIDColumnMap(FTableInfo tableInfo) {
        return IDColumnMap.get(tableInfo);
    }
}
