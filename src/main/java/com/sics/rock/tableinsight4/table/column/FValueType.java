package com.sics.rock.tableinsight4.table.column;

import com.sics.rock.tableinsight4.utils.FTypeUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

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

    public static FValueType createArrayType(FValueType elementType) {
        return new FValueType(FTypeType.ARRAY, new Object[]{elementType});
    }

    public static FValueType createStructureType(List<FValueType> components) {
        return new FValueType(FTypeType.STRUCTURE, components.toArray());
    }

    private FValueType(FTypeType typeType, Object[] typeInfo) {
        this.typeType = typeType;
        this.typeInfo = typeInfo;
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
        BASIC, ARRAY, STRUCTURE
    }

    private enum FBasicType implements Serializable {
        STRING(String.class),
        INTEGER(Integer.class),
        LONG(Long.class),
        DOUBLE(Double.class),
        DATE(java.sql.Date.class),
        TIMESTAMP(java.sql.Timestamp.class),
        BOOLEAN(Boolean.class);

        private final Class<?> jType;

        FBasicType(Class<?> jType) {
            this.jType = jType;
        }

        private static final Set<FBasicType> COMPARABLE_TYPES = FUtils.setOf(INTEGER, LONG, DOUBLE, DATE, TIMESTAMP);

        public boolean isComparable() {
            return COMPARABLE_TYPES.contains(this);
        }
    }
}
