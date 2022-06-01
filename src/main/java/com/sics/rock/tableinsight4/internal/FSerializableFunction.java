package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;
import java.util.function.Function;

public interface FSerializableFunction<T, R> extends Function<T, R>, Serializable {
}
