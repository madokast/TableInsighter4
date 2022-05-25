package com.sics.rock.tableinsight4.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

/**
 * Type cast func used in spark-sql
 *
 * @author zhaorx
 */
public class FTypeUtils {

    private static final String castStr = "asS";
    private static final String castLong = "asL";
    private static final String castDouble = "asD";

    public static void registerSparkUDF(SQLContext sqlContext) {
        sqlContext.udf().register(castStr, FTypeUtils::castStr, DataTypes.StringType);
        sqlContext.udf().register(castLong, FTypeUtils::castLong, DataTypes.LongType);
        sqlContext.udf().register(castDouble, FTypeUtils::castDouble, DataTypes.DoubleType);
    }

    public static String cast(String columnName, Class<?> type) {
        // asX(`c`) AS `c`
        return String.format("%s(`%s`) AS `%s`", udfIdentifierByType(type), columnName, columnName);
    }

    private static String udfIdentifierByType(Class<?> type) {
        if (type.equals(Long.class)) return castLong;
        else if (type.equals(Double.class)) return castDouble;
        else if (type.equals(String.class)) return castStr;
        else throw new IllegalArgumentException("No cast udf matches type " + type);
    }


    private static String castStr(Object val) {
        if (val == null) return null;
        else {
            String str = val.toString();
            if (StringUtils.isBlank(str)) return null;
            else return str;
        }
    }

    private static Long castLong(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof Long) return (Long) val;
            else if (val instanceof Integer) return ((Integer) val).longValue();
            else {
                try {
                    return Long.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static Double castDouble(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof Double) return (Double) val;
            else if (val instanceof Float) return ((Float) val).doubleValue();
            else {
                try {
                    return Double.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }
}
