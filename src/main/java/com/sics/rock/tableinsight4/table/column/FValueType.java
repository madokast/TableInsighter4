package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FTypeUtils;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

/**
 * Type of value in table.
 * basic type
 * typeType = BASIC, typeInfo = [basic_type]
 * <p>
 * array type
 * typeType = ARRAY, typeInfo = [element_type]
 *
 * @author zhaorx
 */
public class FValueType implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FValueType.class);

    public static final FValueType STRING = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.STRING});
    public static final FValueType INTEGER = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.INTEGER});
    public static final FValueType LONG = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.LONG});
    public static final FValueType DOUBLE = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.DOUBLE});
    public static final FValueType DATE = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.DATE});
    public static final FValueType TIMESTAMP = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.TIMESTAMP});
    public static final FValueType BOOLEAN = new FValueType(FTypeType.BASIC, new Object[]{FBasicType.BOOLEAN});

    private final FTypeType typeType;

    private final Object[] typeInfo;

    private final DataType sparkSqlType;

    public static FValueType createArrayType(FValueType elementType) {
        return new FValueType(FTypeType.ARRAY, new Object[]{elementType});
    }

    public FValueType(FTypeType typeType, Object[] typeInfo) {
        this.typeType = typeType;
        this.typeInfo = typeInfo;
        switch (typeType) {
            case BASIC:
                this.sparkSqlType = ((FBasicType) typeInfo[0]).sparkSqlType;
                break;
            case ARRAY:
                this.sparkSqlType = DataTypes.createArrayType(((FValueType) typeInfo[0]).sparkSqlType);
                break;
            default:
                throw new RuntimeException("Unknown typeType " + typeType);
        }
    }

    public Object cast(Object val) {
        if (typeType.equals(FTypeType.BASIC) && typeInfo[0] instanceof FBasicType) {
            return FTypeUtils.cast(val, ((FBasicType) typeInfo[0]).jType).orElse(null);
        } else {
            throw new NotImplementedException("Cannot cast " + val + " to " + this);
        }
    }

    /**
     * @return val is an instance of this type or not
     */
    public boolean instance(Object val) {
        if (typeType.equals(FTypeType.BASIC) && typeInfo[0] instanceof FBasicType) {
            return val == null || ((FBasicType) typeInfo[0]).jType.isAssignableFrom(val.getClass());
        } else {
            throw new NotImplementedException("Cannot infer instance " + val + " of " + this);
        }
    }

    public boolean isComparable() {
        if (typeType.equals(FTypeType.BASIC) && typeInfo[0] instanceof FBasicType) {
            return ((FBasicType) typeInfo[0]).isComparable();
        }
        return false;
    }

    public DataType getSparkSqlType() {
        return sparkSqlType;
    }

    @Override
    public String toString() {
        if (typeType.equals(FTypeType.BASIC)) {
            return typeInfo[0].toString();
        } else if (typeType.equals(FTypeType.ARRAY)) {
            return "Array[" + typeInfo[0] + "]";
        } else {
            return "Struct" + Arrays.toString(typeInfo);
        }
    }


    private enum FTypeType implements Serializable {
        BASIC, ARRAY
    }

    private enum FBasicType implements Serializable {
        STRING(String.class, DataTypes.StringType),
        INTEGER(Integer.class, DataTypes.IntegerType),
        LONG(Long.class, DataTypes.LongType),
        DOUBLE(Double.class, DataTypes.DoubleType),
        DATE(java.sql.Date.class, DataTypes.DateType),
        TIMESTAMP(java.sql.Timestamp.class, DataTypes.TimestampType),
        BOOLEAN(Boolean.class, DataTypes.BooleanType);

        private final Class<?> jType;
        private final DataType sparkSqlType;

        FBasicType(Class<?> jType, DataType sparkSqlType) {
            this.jType = jType;
            this.sparkSqlType = sparkSqlType;
        }

        private static final Set<FBasicType> COMPARABLE_TYPES = FTiUtils.setOf(INTEGER, LONG, DOUBLE, DATE, TIMESTAMP);

        public boolean isComparable() {
            return COMPARABLE_TYPES.contains(this);
        }
    }
}
