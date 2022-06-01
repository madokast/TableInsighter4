package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.junit.After;
import org.junit.Before;

public abstract class FTiTestEnv extends FSparkTestEnv implements FTiEnvironment {

    @Before()
    public void __createTiEnv() {
        // registerSparkUDF
        FTypeUtils.registerSparkUDF(spark.sqlContext());
        FTiEnvironment.create(spark, FTiConfig.defaultConfig());
    }

    @After
    public void __closeTiEnv() {
        FTiEnvironment.destroy();
    }

}
