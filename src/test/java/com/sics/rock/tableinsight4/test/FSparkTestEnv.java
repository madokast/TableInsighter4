package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.utils.FSparkUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class FSparkTestEnv extends FTestTools {

    protected SparkSession spark;

    protected JavaSparkContext sc;

    @Before
    public void __setSpark() {
        // org.apache.spark.sql.AnalysisException: Hive built-in ORC data source must be used with Hive support enabled.
        // Please use the native ORC data source by setting 'spark.sql.orc.impl' to 'native';
        this.spark = FSparkUtils.localSparkSession("spark.sql.orc.impl", "native");
        this.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    }

    @After
    public void __closeSpark() {
        this.sc = null;
        this.spark.close();
        this.spark = null;
        logger.debug("Close SparkSession");
    }

    protected <E> JavaRDD<E> randRDD(Supplier<E> supplier, int size) {
        List<E> data = Stream.generate(supplier).limit(size).collect(Collectors.toList());
        return sc.parallelize(data);
    }

    protected JavaRDD<Integer> randIntRdd(int size) {
        return randRDD(random::nextInt, size);
    }

    @SafeVarargs
    protected final <E> JavaRDD<E> rddOf(E... es) {
        return sc.parallelize(FUtils.listOf(es));
    }

    // --------------- hadoop env ------------------------

    static {
        // run once
        setHadoopEnv();
    }

    private static void setHadoopEnv() {
        if (!System.getProperty("HADOOP_HOME", "null").equals("null")) {
            return;
        }

        File hadoopDir = new File(findRootDir(), "hadoop");
        String hadoopHome = hadoopDir.getAbsolutePath();
        logger.debug("hadoopHome={}", hadoopHome);
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
