package com.sics.rock.tableinsight4.env;

import java.util.Objects;

/**
 * The draft implementation of EnvironmentUser.
 * Identifying user by userKey()
 *
 * @author zhaorx
 */
public abstract class FAbstractEnvironmentUser implements FIEnvironmentUser{

    @Override
    public final int hashCode() {
        return Objects.hashCode(userKey());
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof FIEnvironmentUser)) return false;
        FIEnvironmentUser that = (FIEnvironmentUser) obj;
        return userKey().equals(that.userKey());
    }
}
