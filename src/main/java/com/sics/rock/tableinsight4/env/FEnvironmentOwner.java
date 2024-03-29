package com.sics.rock.tableinsight4.env;

/**
 * environment owner
 *
 * @author zhaorx
 */
public class FEnvironmentOwner extends FEnvironmentSharer {

    private FEnvironmentOwner(Thread owner) {
        super(owner);
    }

    public static FEnvironmentOwner current() {
        return new FEnvironmentOwner(Thread.currentThread());
    }
}
