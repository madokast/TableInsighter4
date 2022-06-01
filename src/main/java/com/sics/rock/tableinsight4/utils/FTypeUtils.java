package com.sics.rock.tableinsight4.utils;

import com.sics.rock.tableinsight4.internal.FSerializableFunction;
import com.sics.rock.tableinsight4.table.column.FValueType;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Safe type cast func
 * Use in spark-sql
 * Use in config parse
 *
 * @author zhaorx
 */
public class FTypeUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTypeUtils.class);

    private static final List<FCaster<?>> ALL_CASTER = new ArrayList<>();
    private static final Map<FValueType, FCaster<?>> TYPE_CASTER_MAP = new HashMap<>();
    private static final Map<Class<?>, FCaster<?>> KLASS_CASTER_MAP = new HashMap<>();

    public synchronized static void registerSparkUDF(SQLContext sqlContext) {
        logger.info("Register spark udf");
        final UDFRegistration registration = sqlContext.udf();
        ALL_CASTER.forEach(caster -> registration.register(caster.castFuncName, caster.caster::apply, caster.targetType));
    }

    public static String castSQLClause(String columnName, FValueType type) {
        final String castFuncName = TYPE_CASTER_MAP.get(type).castFuncName;
        // asX(`c`) AS `c`
        return String.format("%s(`%s`) AS `%s`", castFuncName, columnName, columnName);
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> cast(Object value, Class<T> target) {
        final T casted = (T) KLASS_CASTER_MAP.get(target).caster.apply(value);
        return Optional.ofNullable(casted);
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

    private static java.sql.Timestamp castTimestamp(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof java.sql.Timestamp) return (java.sql.Timestamp) val;
            else {
                try {

                    return java.sql.Timestamp.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static java.sql.Date castDate(Object val) {
        if (val == null) return null;
        else {
            if (val instanceof java.sql.Date) return (java.sql.Date) val;
            else {
                try {
                    return java.sql.Date.valueOf(val.toString());
                } catch (Exception e) {
                    return null;
                }
            }
        }
    }

    private static Double[] castDoubleArray(Object val) {
        throw new UnsupportedOperationException("Can not cast to double[] type");
    }

    public static String toString(Class<?> type) {
        String str = type.toString();
        int lastDot = str.lastIndexOf(".");
        if (lastDot == -1) return str;
        else return str.substring(lastDot + 1);
    }

    static DataType toSparkDataType(Class<?> klass) {
        if (klass == null) throw new NullPointerException("Can not infer a type of null");
        return KLASS_CASTER_MAP.get(klass).targetType;
    }

    private static class FCaster<T> {
        private final String castFuncName;
        private final Class<T> targetClass;
        private final FValueType valueType;
        private final DataType targetType;
        private final FSerializableFunction<Object, T> caster;

        FCaster(String castFuncName, Class<T> targetClass, FValueType valueType, DataType targetType, FSerializableFunction<Object, T> caster) {
            this.castFuncName = castFuncName;
            this.targetClass = targetClass;
            this.valueType = valueType;
            this.targetType = targetType;
            this.caster = caster;
        }

        public static <T> FCaster<T> of(String castFuncName, Class<T> targetClass, FValueType valueType, DataType dataType, FSerializableFunction<Object, T> caster) {
            return new FCaster<>(castFuncName, targetClass, valueType, dataType, caster);
        }
    }

    // init
    static {
        final FCaster<Boolean> CAST_BOOLEAN = FCaster.of("asB", Boolean.class, FValueType.BOOLEAN, DataTypes.BooleanType, FTypeUtils::castBoolean);
        final FCaster<Integer> CAST_INTEGER = FCaster.of("asI", Integer.class, FValueType.INTEGER, DataTypes.IntegerType, FTypeUtils::castInteger);
        final FCaster<Long> CAST_LONG = FCaster.of("asL", Long.class, FValueType.LONG, DataTypes.LongType, FTypeUtils::castLong);
        final FCaster<Double> CAST_DOUBLE = FCaster.of("asD", Double.class, FValueType.DOUBLE, DataTypes.DoubleType, FTypeUtils::castDouble);
        final FCaster<String> CAST_STR = FCaster.of("asS", String.class, FValueType.STRING, DataTypes.StringType, FTypeUtils::castStr);
        final FCaster<java.sql.Date> CAST_DATE = FCaster.of("asDt", java.sql.Date.class, FValueType.DATE, DataTypes.DateType, FTypeUtils::castDate);
        final FCaster<java.sql.Timestamp> CAST_TIMESTAMP = FCaster.of("asTS", java.sql.Timestamp.class, FValueType.TIMESTAMP, DataTypes.TimestampType, FTypeUtils::castTimestamp);

        final FCaster<Double[]> CAST_DOUBLE_ARRAY = FCaster.of("asD_A", Double[].class, FValueType.createArrayType(FValueType.DOUBLE), DataTypes.createArrayType(DataTypes.DoubleType), FTypeUtils::castDoubleArray);

        ALL_CASTER.add(CAST_BOOLEAN);
        ALL_CASTER.add(CAST_INTEGER);
        ALL_CASTER.add(CAST_LONG);
        ALL_CASTER.add(CAST_DOUBLE);
        ALL_CASTER.add(CAST_STR);
        ALL_CASTER.add(CAST_DATE);
        ALL_CASTER.add(CAST_TIMESTAMP);
        ALL_CASTER.add(CAST_DOUBLE_ARRAY);

        ALL_CASTER.forEach(caster -> TYPE_CASTER_MAP.put(caster.valueType, caster));
        ALL_CASTER.forEach(caster -> KLASS_CASTER_MAP.put(caster.targetClass, caster));
    }
}
