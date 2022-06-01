package com.sics.rock.tableinsight4.table.column;

import java.io.Serializable;
import java.util.List;

public class FValueType implements Serializable {

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

    private enum FTypeType implements Serializable {
        BASIC, ARRAY, STRUCTURE
    }

    private enum FBasicType implements Serializable {
        STRING, INTEGER, LONG, DOUBLE, DATE, TIMESTAMP, BOOLEAN
    }
}
