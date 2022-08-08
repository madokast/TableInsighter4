package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The env holder.
 * <p>
 * An owner is a sharer but a sharer is not an owner.
 * <p>
 * The owner can create and destroy his env.
 * The owner can share his env to sharer.
 *
 * @author zhaorx
 */
final class FTiEnvironmentHolder {

    static Logger logger = LoggerFactory.getLogger(FTiEnvironment.class);

    private static Map<FEnvironmentOwner, SparkSession> SPARK_SESSION_MAP = new ConcurrentHashMap<>();

    private static Map<FEnvironmentOwner, FTiConfig> CONFIG_MAP = new ConcurrentHashMap<>();

    private static Map<FEnvironmentSharer, FEnvironmentOwner> SHARER_OWNER_MAP = new ConcurrentHashMap<>();

    static void put(FEnvironmentOwner owner, SparkSession spark, FTiConfig config) {
        SPARK_SESSION_MAP.put(owner, spark);
        CONFIG_MAP.put(owner, config);
        // Only sharers can access env. The owner need share his env to himself to access it.
        share(owner, owner);
        clean();
    }

    static void removeAll(FEnvironmentOwner user) {
        stopSharing(user);
        SPARK_SESSION_MAP.remove(user);
        CONFIG_MAP.remove(user);
        clean();
    }

    static void share(FEnvironmentOwner owner, FEnvironmentSharer sharer) {
        FAssertUtils.require(() -> !SHARER_OWNER_MAP.containsKey(sharer), () -> "The env has been shared to " + sharer);
        FAssertUtils.require(() -> SPARK_SESSION_MAP.containsKey(owner), () -> "The owner " + owner + " does not hold any env!");
        FAssertUtils.require(() -> CONFIG_MAP.containsKey(owner), () -> "The owner " + owner + " does not hold any env!");
        SHARER_OWNER_MAP.put(sharer, owner);
    }

    static void stopSharing(FEnvironmentSharer sharer) {
        final FEnvironmentOwner owner = SHARER_OWNER_MAP.remove(sharer);
        FAssertUtils.require(owner != null, () -> "The sharer returns back env but no owner linking to");
    }

    static SparkSession getSparkSession(FEnvironmentSharer user) {
        final FEnvironmentOwner owner = SHARER_OWNER_MAP.get(user);
        if (owner == null) throw new RuntimeException(new IllegalAccessException("No owner linked to " + user));
        return SPARK_SESSION_MAP.get(owner);
    }

    static FTiConfig getConfig(FEnvironmentSharer user) {
        final FEnvironmentOwner owner = SHARER_OWNER_MAP.get(user);
        if (owner == null) throw new RuntimeException(new IllegalAccessException("No owner linked to " + user));
        return CONFIG_MAP.get(owner);
    }

    /**
     * Release orphaned env
     */
    private static void clean() {
        for (FEnvironmentOwner owner : SPARK_SESSION_MAP.keySet()) {
            if (!owner.isAlive()) {
                logger.warn("The owner " + owner + " is dead but has not destroyed the env", new RuntimeException());
                SPARK_SESSION_MAP.remove(owner);
            }
        }

        for (FEnvironmentOwner owner : CONFIG_MAP.keySet()) {
            if (!owner.isAlive()) {
                logger.warn("The owner " + owner + " is dead but has not destroyed the env", new RuntimeException());
                CONFIG_MAP.remove(owner);
            }
        }

        for (FEnvironmentSharer sharer : SHARER_OWNER_MAP.keySet()) {
            if (!sharer.isAlive()) {
                logger.warn("The sharer " + sharer + " is dead but has not stop the linker", new RuntimeException());
                SHARER_OWNER_MAP.remove(sharer);
            }
        }
    }

    static boolean linked(FEnvironmentSharer sharer, FEnvironmentOwner owner) {
        return SHARER_OWNER_MAP.get(sharer).equals(owner);
    }

    private FTiEnvironmentHolder() {
    }
}
