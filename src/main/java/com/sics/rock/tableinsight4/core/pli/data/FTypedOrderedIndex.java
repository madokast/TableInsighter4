package com.sics.rock.tableinsight4.core.pli.data;

import com.sics.rock.tableinsight4.table.column.FValueType;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class FTypedOrderedIndex {

    private static final Logger logger = LoggerFactory.getLogger(FTypedOrderedIndex.class);

    private final Map<FValueType, JavaPairRDD<Object, Long>> indexMap;

    public FTypedOrderedIndex(Map<FValueType, JavaPairRDD<Object, Long>> indexMap) {
        this.indexMap = indexMap;
    }

    public Optional<JavaPairRDD<Object, Long>> indexesOf(FValueType valueType) {
        return Optional.ofNullable(indexMap.getOrDefault(valueType, null));
    }
}
