package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class FTiEnvironmentHolder {

    static Logger logger = LoggerFactory.getLogger(FTiEnvironment.class);

    private static Map<Thread, SparkSession> SPARK_SESSION_MAP = new ConcurrentHashMap<>();

    private static Map<Thread, FTiConfig> CONFIG_MAP = new ConcurrentHashMap<>();

    static void put(Thread key, SparkSession spark, FTiConfig config) {
        SPARK_SESSION_MAP.put(key, spark);
        CONFIG_MAP.put(key, config);

        clean();
    }

    static void removeAll(Thread key) {
        SPARK_SESSION_MAP.remove(key);
        CONFIG_MAP.remove(key);

        clean();
    }

    static SparkSession getSparkSession(Thread key){
        return SPARK_SESSION_MAP.get(key);
    }

    static FTiConfig getConfig(Thread key){
        return CONFIG_MAP.get(key);
    }


    /**
     * Release orphaned env
     */
    private static void clean() {
        for (Thread thread : SPARK_SESSION_MAP.keySet()) {
            if (!thread.isAlive()) {
                logger.warn(thread.getName() + " is dead but has not destroyed/returned back the env", new RuntimeException());
                SPARK_SESSION_MAP.remove(thread);
            }
        }

        for (Thread thread : CONFIG_MAP.keySet()) {
            if (!thread.isAlive()) {
                logger.warn(thread.getName() + " is dead but has not destroyed/returned back the env", new RuntimeException());
                CONFIG_MAP.remove(thread);
            }
        }
    }

}
