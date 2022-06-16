package com.sics.rock.tableinsight4.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Asset utils
 *
 * @author zhaorx
 */
public final class FAssertUtils {

    private static final Logger logger = LoggerFactory.getLogger(FAssertUtils.class);

    private static final boolean ASSERT = true;

    public static void require(BooleanSupplier object, Supplier<String> msg) {
        if (ASSERT) {
            if (!object.getAsBoolean()) {
                throw new AssertionError(msg.get());
            }
        }
    }

    public static <ARGS> void require(Predicate<ARGS> object, Function<ARGS, String> msg, ARGS args) {
        if (ASSERT) {
            if (!object.test(args)) {
                throw new AssertionError(msg.apply(args));
            }
        }
    }

    public static void require(boolean object, Supplier<String> msg) {
        if (ASSERT) {
            if (!object) {
                throw new AssertionError(msg.get());
            }
        }
    }

    public static void require(BooleanSupplier object, String msg) {
        if (ASSERT) {
            if (!object.getAsBoolean()) {
                throw new AssertionError(msg);
            }
        }
    }

    public static void require(boolean object, String msg) {
        if (ASSERT) {
            if (!object) {
                throw new AssertionError(msg);
            }
        }
    }

    static {
        if (ASSERT) {
            logger.info("■■■■■■■■■■■■■ ASSERT MODE ■■■■■■■■■■■■■");
        }
    }
}
