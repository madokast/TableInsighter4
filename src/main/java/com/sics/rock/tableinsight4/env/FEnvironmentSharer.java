package com.sics.rock.tableinsight4.env;

public class FEnvironmentSharer extends FAbstractEnvironmentUser implements FIEnvironmentUser {

    private final Thread sharer;

    FEnvironmentSharer(Thread sharer) {
        this.sharer = sharer;
    }

    static FEnvironmentSharer current() {
        return new FEnvironmentSharer(Thread.currentThread());
    }

    @Override
    public boolean isAlive() {
        return sharer.isAlive();
    }

    @Override
    public Object userKey() {
        return sharer;
    }

    @Override
    public String toString() {
        return sharer.getName();
    }
}
