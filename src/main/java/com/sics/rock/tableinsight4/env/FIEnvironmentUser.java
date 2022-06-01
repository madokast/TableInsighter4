package com.sics.rock.tableinsight4.env;

public interface FIEnvironmentUser {

    Object userKey();

    boolean isAlive();

    @Override
    boolean equals(Object o);

    @Override
    int hashCode();

    @Override
    String toString();
}
