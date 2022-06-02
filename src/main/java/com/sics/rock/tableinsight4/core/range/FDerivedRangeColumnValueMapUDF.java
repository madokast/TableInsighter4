package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.util.List;

/**
 * UDF for generation of derived range column
 *
 * @author zhaorx
 */
public class FDerivedRangeColumnValueMapUDF implements UDF1<Object, String> {

    // TODO FRange
    private final Broadcast<List<FPair<FRange, String>>> rangeMapHandle;

    /**
     * transient
     * Get value by Broadcast rangeMapHandle
     */
    private transient List<FPair<FRange, String>> rangeMap;

    private FDerivedRangeColumnValueMapUDF(List<FPair<FRange, String>> rangeMap, JavaSparkContext sc) {
        this.rangeMapHandle = sc.broadcast(rangeMap);
    }

    @Override
    public String call(Object val) {
        if (val == null) return null;
        FAssertUtils.require(() -> val instanceof Number, () -> val + " is not a number");
        final double v = ((Number) val).doubleValue();
        checkMap();
        for (FPair<FRange, String> rangeStr : rangeMap) {
            if (rangeStr._k.including(v)) return rangeStr._v;
        }
        return null;
    }

    private void checkMap() {
        if (rangeMap == null) {
            rangeMap = rangeMapHandle.getValue();
        }
    }
}
