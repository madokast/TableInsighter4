package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.test.FSparkTestEnv;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class FTiEnvironmentTest extends FSparkTestEnv {

    @Test
    public void test_create_destroy() {
        final FTiConfig config = FTiConfig.defaultConfig();
        FTiEnvironment.create(spark, config);
        final FTiEnvironment env = new FTiEnvironment() {
        };
        assertEquals(config, env.config());
        FTiEnvironment.destroy();
    }

    @Test
    public void test_create_destroy2() {
        IntStream.range(0, 20).parallel().forEach(i -> {
            final FTiConfig config = FTiConfig.defaultConfig();
            FTiEnvironment.create(spark, config);
            final FTiEnvironment env = new FTiEnvironment() {
            };
            assertEquals(config, env.config());
            try {
                TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            FTiEnvironment.destroy();
        });
    }

    @Test
    public void test_share() {
        final FTiConfig config = FTiConfig.defaultConfig();
        FTiEnvironment.create(spark, config);
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        IntStream.range(0, 20).parallel().forEach(i->{
            FTiEnvironment.shareFrom(owner);
            final FTiEnvironment env = new FTiEnvironment() {
            };
            assertEquals(config, env.config());
            FTiEnvironment.returnBack(owner);
        });

        FTiEnvironment.destroy();
    }

    @Test(expected = AssertionError.class)
    public void test_share_but_not_return() {
        final FTiConfig config = FTiConfig.defaultConfig();
        FTiEnvironment.create(spark, config);
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        IntStream.range(0, 40).parallel().forEach(i->{
            FTiEnvironment.shareFrom(owner);
            // FTiEnvironment.returnBack(owner);
        });

        FTiEnvironment.destroy();
    }

    @Test(expected = Exception.class)
    public void test_just_get() {
        final FTiConfig config = FTiConfig.defaultConfig();
        FTiEnvironment.create(spark, config);
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        IntStream.range(0, 40).parallel().forEach(i->{
            final FTiEnvironment env = new FTiEnvironment() {
            };
            final FTiConfig nil = env.config();
        });

        FTiEnvironment.destroy();
    }

}