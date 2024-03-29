package com.sics.rock.tableinsight4.env;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FAssertUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class FTiEnvironmentTest extends FSparkEnv {

    @Parameterized.Parameters
    public static Object[][] __() {
        return new Object[10][0];
    }

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
        IntStream.range(0, 20).parallel().forEach(i -> {
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
        if (FAssertUtils.ASSERT) {
            final FTiConfig config = FTiConfig.defaultConfig();
            FTiEnvironment.create(spark, config);
            final FEnvironmentOwner owner = FEnvironmentOwner.current();
            IntStream.range(0, 400).parallel().forEach(i -> {
                FTiEnvironment.shareFrom(owner);
                // FTiEnvironment.returnBack(owner);
            });

            FTiEnvironment.destroy();
        } else {
            throw new AssertionError("The test works only in FAssertUtils.ASSERT on");
        }
    }

    @Test(expected = Exception.class)
    public void test_just_get() {
        final FTiConfig config = FTiConfig.defaultConfig();
        FTiEnvironment.create(spark, config);
        final FEnvironmentOwner owner = FEnvironmentOwner.current();
        IntStream.range(0, 400).parallel().forEach(i -> {
            final FTiEnvironment env = new FTiEnvironment() {
            };
            final FTiConfig nil = env.config();
        });

        FTiEnvironment.destroy();
    }

    /**
     * Destroy all env because some tests here do not obey the correct specifications.
     */
    @After
    public void destroyAll() throws Throwable {
        logger.info("destroyAll Env");
        for (Field field : FTiEnvironmentHolder.class.getDeclaredFields()) {
            if (Map.class.isAssignableFrom(field.getType())) {
                field.setAccessible(true);
                final Map<?, ?> map = (Map) field.get(null);
                if (map.getClass().equals(ConcurrentHashMap.class)) {
                    map.clear();
                    logger.info("Clean {}", field.getName());
                }
                field.setAccessible(false);
            }
        }
    }

}