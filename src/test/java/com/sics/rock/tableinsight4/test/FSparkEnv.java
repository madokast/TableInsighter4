package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.utils.FSparkUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Objects;

public class FSparkEnv {
    private static final Logger logger = LoggerFactory.getLogger(FSparkEnv.class);

    public SparkSession spark;

    public JavaSparkContext sc;

    @Before
    public void setSpark() {
        this.spark = FSparkUtils.localSparkSession();
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    @After
    public void after() {
        this.sc = null;
        this.spark.close();
        this.spark = null;
        logger.info("Close SparkSession");
    }

    // --------------- hadoop env ------------------------

    @BeforeClass
    public static void setHadoopEnv() {
        if (!System.getProperty("HADOOP_HOME", "null").equals("null")) {
            return;
        }

        File hadoopDir = new File(findRootDir(), "hadoop");
        String hadoopHome = hadoopDir.getAbsolutePath();
        logger.info("hadoopHome={}", hadoopHome);
        System.setProperty("HADOOP_HOME", hadoopHome);
        System.setProperty("hadoop.home.dir", hadoopHome);
    }

    private static String findRootDir() {
        URL rootResource = LoggerFactory.class.getClassLoader().getResource(".");
        Objects.requireNonNull(rootResource, "Cannot find root path !!");
        String maybeRoot = rootResource.getFile();
        logger.debug("maybeRoot {}", maybeRoot);

        String rootDir = null;

        if (new File(maybeRoot, "hadoop").exists()) {
            rootDir = maybeRoot;
        }

        if (rootDir == null) {
            throw new RuntimeException(new FileNotFoundException("Cannot find root path !!"));
        } else {
            logger.debug("Find root path {}", rootDir);
            return rootDir;
        }
    }
}
