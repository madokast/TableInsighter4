package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime env for table-insight4
 * Components implementing it can get spark or ti-conf conveniently
 * <p>
 * create()         destroy()
 * shareFrom(owner) returnBack(owner)
 *
 * <p>
 * Do not access it in spark executors
 *
 * @author zhaorx
 */
public interface FTiEnvironment {

    Logger __LOG = LoggerFactory.getLogger(FTiEnvironment.class);

    Map<Thread, SparkSession> __SPARK_SESSION_MAP = new ConcurrentHashMap<>();

    Map<Thread, FTiConfig> __CONFIG_MAP = new ConcurrentHashMap<>();

    default FTiConfig config() {
        FTiConfig conf = __CONFIG_MAP.get(Thread.currentThread());
        return Objects.requireNonNull(conf, "Current environment has not been created or has been closed");
    }

    default SparkSession spark() {
        SparkSession spark = __SPARK_SESSION_MAP.get(Thread.currentThread());
        return Objects.requireNonNull(spark, "Current environment has not been created or has been closed");
    }

    static void create(SparkSession spark, FTiConfig config) {
        final Thread ct = Thread.currentThread();
        __LOG.info("Create Ti-Env of {}", ct.getName());
        // registerSparkUDF
        FTypeUtils.registerSparkUDF(spark.sqlContext());
        __SPARK_SESSION_MAP.put(ct, spark);
        __CONFIG_MAP.put(ct, config);

        __clean();
    }

    static void destroy() {
        final Thread ct = Thread.currentThread();
        __LOG.info("Destroy Ti-Env of {}", ct.getName());
        __SPARK_SESSION_MAP.remove(ct);
        __CONFIG_MAP.remove(ct);

        __clean();
    }

    static void shareFrom(Thread owner) {
        final Thread ct = Thread.currentThread();
        if (ct == owner) return;
        __LOG.info("{} share env to {}", owner.getName(), ct.getName());
        final SparkSession spark = __SPARK_SESSION_MAP.get(owner);
        final FTiConfig config = __CONFIG_MAP.get(owner);

        __SPARK_SESSION_MAP.put(ct, spark);
        __CONFIG_MAP.put(ct, config);
    }

    static void returnBack(Thread owner) {
        final Thread ct = Thread.currentThread();
        if (ct == owner) return;
        __LOG.info("{} return env to {}", ct.getName(), owner.getName());

        FAssertUtils.require(__SPARK_SESSION_MAP.get(owner) == __SPARK_SESSION_MAP.get(ct),
                () -> "The env owner " + owner.getName() + " and the sharer " + ct.getName() + " do not share the same env!");

        FAssertUtils.require(__CONFIG_MAP.get(owner) == __CONFIG_MAP.get(ct),
                () -> "The env owner " + owner.getName() + " and the sharer " + ct.getName() + " do not share the same env!");

        __SPARK_SESSION_MAP.remove(ct);
        __CONFIG_MAP.remove(ct);
    }

    /**
     * Release orphaned env
     */
    static void __clean() {
        for (Thread thread : __SPARK_SESSION_MAP.keySet()) {
            if (!thread.isAlive()) {
                __LOG.warn(thread.getName() + " is dead but has not destroyed/returned back the env", new RuntimeException());
                __SPARK_SESSION_MAP.remove(thread);
            }
        }

        for (Thread thread : __CONFIG_MAP.keySet()) {
            if (!thread.isAlive()) {
                __LOG.warn(thread.getName() + " is dead but has not destroyed/returned back the env", new RuntimeException());
                __CONFIG_MAP.remove(thread);
            }
        }
    }

}
