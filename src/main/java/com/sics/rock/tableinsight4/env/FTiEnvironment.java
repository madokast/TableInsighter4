package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.apache.spark.sql.SparkSession;

import java.util.Objects;

import static com.sics.rock.tableinsight4.env.FTiEnvironmentHolder.logger;

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
        FTiConfig conf = FTiEnvironmentHolder.getConfig(FEnvironmentSharer.current());
        return Objects.requireNonNull(conf, "Current environment has not been created/shared or has been closed");
    }

    default SparkSession spark() {
        SparkSession spark = FTiEnvironmentHolder.getSparkSession(FEnvironmentSharer.current());
        return Objects.requireNonNull(spark, "Current environment has not been created/shared or has been closed");
    }

    /**
     * create a environment belongs to current thread
     */
    static void create(SparkSession spark, FTiConfig config) {
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        logger.info("Create environment of {}", owner);
        FTiEnvironmentHolder.put(owner, spark, config);
    }

    static void destroy() {
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        logger.info("Destroy environment of {}", owner);
        FTiEnvironmentHolder.removeAll(owner);
    }

    static void shareFrom(FEnvironmentOwner owner) {
        final FEnvironmentSharer sharer = FEnvironmentSharer.current();
        if (owner.equals(sharer)) return;
        logger.info("The owner {} shares env to {}", owner, sharer);
        FTiEnvironmentHolder.share(owner, sharer);
    }

    static void returnBack(FEnvironmentOwner owner) {
        final FEnvironmentSharer sharer = FEnvironmentSharer.current();
        if (owner.equals(sharer)) return;
        FAssertUtils.require(() -> FTiEnvironmentHolder.linked(sharer, owner),
                () -> "The share " + sharer + " and the owner " + owner + " are not linked.");
        logger.info("The sharer {} returns env to {}", sharer, owner);
        FTiEnvironmentHolder.stopSharing(sharer);
    }

}
