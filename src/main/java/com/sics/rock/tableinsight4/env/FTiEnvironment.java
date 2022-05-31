package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.spark.sql.SparkSession;

import static com.sics.rock.tableinsight4.env.FTiEnvironmentHolder.logger;

import java.util.Objects;

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

    default FTiConfig config() {
        FTiConfig conf = FTiEnvironmentHolder.getConfig(Thread.currentThread());
        return Objects.requireNonNull(conf, "Current environment has not been created or has been closed");
    }

    default SparkSession spark() {
        SparkSession spark = FTiEnvironmentHolder.getSparkSession(Thread.currentThread());
        return Objects.requireNonNull(spark, "Current environment has not been created or has been closed");
    }

    static void create(SparkSession spark, FTiConfig config) {
        final Thread ct = Thread.currentThread();
        logger.info("Create Ti-Env of {}", ct.getName());
        // registerSparkUDF
        FTypeUtils.registerSparkUDF(spark.sqlContext());
        FTiEnvironmentHolder.put(ct, spark, config);
    }

    static void destroy() {
        final Thread ct = Thread.currentThread();
        logger.info("Destroy Ti-Env of {}", ct.getName());
        FTiEnvironmentHolder.removeAll(ct);
    }

    static void shareFrom(Thread owner) {
        final Thread ct = Thread.currentThread();
        if (ct == owner) return;
        logger.info("{} share env to {}", owner.getName(), ct.getName());
        final SparkSession spark = FTiEnvironmentHolder.getSparkSession(owner);
        final FTiConfig config = FTiEnvironmentHolder.getConfig(owner);

        FTiEnvironmentHolder.put(ct, spark, config);
    }

    static void returnBack(Thread owner) {
        final Thread ct = Thread.currentThread();
        if (ct == owner) return;
        logger.info("{} return env to {}", ct.getName(), owner.getName());

        FAssertUtils.require(() -> FTiEnvironmentHolder.getSparkSession(owner) == FTiEnvironmentHolder.getSparkSession(ct),
                () -> "The env owner " + owner.getName() + " and the sharer " + ct.getName() + " do not share the same env!");

        FAssertUtils.require(() -> FTiEnvironmentHolder.getConfig(owner) == FTiEnvironmentHolder.getConfig(ct),
                () -> "The env owner " + owner.getName() + " and the sharer " + ct.getName() + " do not share the same env!");

        FTiEnvironmentHolder.removeAll(ct);
    }

}
