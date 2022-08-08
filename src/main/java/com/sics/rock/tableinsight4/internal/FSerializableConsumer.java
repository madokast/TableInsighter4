package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;
import java.util.function.Consumer;

/**
 * a consumer for spark operator
 *
 * @author zhaorx
 */
public interface FSerializableConsumer<T> extends Consumer<T>, Serializable {
}
