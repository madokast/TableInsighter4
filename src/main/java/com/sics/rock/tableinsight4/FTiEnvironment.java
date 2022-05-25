package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import org.apache.spark.sql.SparkSession;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public interface FTiEnvironment {

    Map<Thread, SparkSession> SPARK_SESSION_MAP = new ConcurrentHashMap<>();

    Map<Thread, FTiConfig> CONFIG_MAP = new ConcurrentHashMap<>();

    default FTiConfig config() {
        FTiConfig conf = CONFIG_MAP.get(Thread.currentThread());
        return Objects.requireNonNull(conf, "Current environment has not been created or has been closed");
    }

    default SparkSession spark() {
        SparkSession spark = SPARK_SESSION_MAP.get(Thread.currentThread());
        return Objects.requireNonNull(spark, "Current environment has not been created or has been closed");
    }

    static void create(SparkSession spark, FTiConfig config) {
        SPARK_SESSION_MAP.put(Thread.currentThread(), spark);
        CONFIG_MAP.put(Thread.currentThread(), config);

        clean();
    }

    static void destroy() {
        SPARK_SESSION_MAP.remove(Thread.currentThread());
        CONFIG_MAP.remove(Thread.currentThread());

        clean();
    }

    static void clean() {
        for (Thread thread : SPARK_SESSION_MAP.keySet()) {
            if (!thread.isAlive()) SPARK_SESSION_MAP.remove(thread);
        }

        for (Thread thread : CONFIG_MAP.keySet()) {
            if (!thread.isAlive()) CONFIG_MAP.remove(thread);
        }
    }


}
