package com.sics.rock.tableinsight4;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Objects;

public class HadoopEnv4Junit extends BlockJUnit4ClassRunner {

    private static final Logger logger = LoggerFactory.getLogger(HadoopEnv4Junit.class);

    public HadoopEnv4Junit(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    public void run(RunNotifier notifier) {
        setHadoopEnv();
        super.run(notifier);
    }

    private static void setHadoopEnv() {
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
