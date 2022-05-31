package com.sics.rock.tableinsight4.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

/**
 * Type cast func
 * Use in spark-sql
 * Use in config parse
 *
 * @author zhaorx
 */
public class FTypeUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTypeUtils.class);

    private static final String castStr = "asS";
    private static final String castLong = "asL";
    private static final String castDouble = "asD";

    public synchronized static void registerSparkUDF(SQLContext sqlContext) {
        logger.info("Register spark udf");
        sqlContext.udf().register(castStr, FTypeUtils::castStr, DataTypes.StringType);
        sqlContext.udf().register(castLong, FTypeUtils::castLong, DataTypes.LongType);
        sqlContext.udf().register(castDouble, FTypeUtils::castDouble, DataTypes.DoubleType);
    }

    public static String castSQLClause(String columnName, Class<?> type) {
        // asX(`c`) AS `c`
        return String.format("%s(`%s`) AS `%s`", udfIdentifierByType(type), columnName, columnName);
    }


    private static String udfIdentifierByType(Class<?> type) {
        if (type.equals(Long.class)) return castLong;
        else if (type.equals(Double.class)) return castDouble;
        else if (type.equals(String.class)) return castStr;
        else throw new IllegalArgumentException("No cast udf matches type " + type);
    }


    @SuppressWarnings("unchecked")
    public static <T> Optional<T> cast(Object value, Class<T> target) {
        T result;
        if (target.equals(String.class)) result = (T) castStr(value);
        else if (target.equals(Long.class)) result = (T) castLong(value);
        else if (target.equals(Integer.class)) result = (T) castInteger(value);
        else if (target.equals(Double.class)) result = (T) castDouble(value);
        else if (target.equals(Boolean.class)) result = (T) castBoolean(value);
        else throw new IllegalArgumentException("Cannot cast " + value + " to " + target);

        return Optional.ofNullable(result);
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
            if (val instanceof Number) return ((Number) val).longValue();
            else {
                try {
                    return Long.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static Integer castInteger(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof Number) return ((Number) val).intValue();
            else {
                try {
                    return Integer.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static Double castDouble(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof Number) return ((Number) val).doubleValue();
            else {
                try {
                    return Double.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static Boolean castBoolean(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof Boolean) return (Boolean) val;
            else {
                try {
                    return Boolean.parseBoolean(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    public static String toString(Class<?> type) {
        String str = type.toString();
        int lastDot = str.lastIndexOf(".");
        if (lastDot == -1) return str;
        else return str.substring(lastDot + 1);
    }

    public static DataType toSparkDataType(Class<?> klass) {
        if (klass == null) throw new NullPointerException("Can not infer a type of null");
        if (klass.equals(String.class)) return DataTypes.StringType;
        else if (klass.equals(Double.class)) return DataTypes.DoubleType;
        else if (klass.equals(Long.class)) return DataTypes.LongType;
        else if (klass.equals(Integer.class)) return DataTypes.IntegerType;
        else if (klass.equals(Boolean.class)) return DataTypes.BooleanType;
        else if (klass.equals(Float.class)) return DataTypes.FloatType;
        else throw new IllegalArgumentException("Unknown type " + klass);
    }
}
