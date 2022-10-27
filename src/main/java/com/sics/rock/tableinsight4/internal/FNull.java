package com.sics.rock.tableinsight4.internal;

import java.io.Serializable;

/**
 * null val
 *
 * @author zhaorx
 */
public final class FNull implements Serializable {

    public static final FNull VALUE = null;

    public static FNull merge(FNull n1, FNull n2) {
        return VALUE;
    }

    private FNull() {
    }
}
