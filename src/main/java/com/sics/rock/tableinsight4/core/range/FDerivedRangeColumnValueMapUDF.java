package com.sics.rock.tableinsight4.core.range;

import com.sics.rock.tableinsight4.internal.FPair;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.api.java.UDF1;

import java.util.Map;

/**
 * UDF for generation of derived range column
 * @author zhaorx
 */
public class FDerivedRangeColumnValueMapUDF implements UDF1<Object, String> {

    // TODO FRange
    private final Broadcast<Map<FPair<Double, Double>, String>> rangeMapHandle;

    /**
     * transient
     * Get value by Broadcast rangeMapHandle
     */
    private transient Map<FPair<Double, Double>, String> rangeMap;

    private FDerivedRangeColumnValueMapUDF(Map<FPair<Double, Double>, String> rangeMap, JavaSparkContext sc) {
        this.rangeMapHandle = sc.broadcast(rangeMap);
    }

    @Override
    public String call(Object val) {
        if (val == null) return null;
        FAssertUtils.require(() -> val instanceof Number, () -> val + " is not a number");
        final double v = ((Number) val).doubleValue();
        checkMap();
        for (Map.Entry<FPair<Double, Double>, String> e : rangeMap.entrySet()) {
            final FPair<Double, Double> range = e.getKey();
            final Double left = range._k;
            final Double right = range._v;
            if (left.equals(Double.NEGATIVE_INFINITY)) {
                if (v < right) return e.getValue();
            } else if (right.equals(Double.POSITIVE_INFINITY)) {
                if (v >= left) return e.getValue();
            } else {
                if (v >= left && v < right) return e.getValue();
            }
        }
        return null;
    }

    private void checkMap() {
        if (rangeMap == null) {
            rangeMap = rangeMapHandle.getValue();
        }
    }
}
