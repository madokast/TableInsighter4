package com.sics.rock.tableinsight4.test;

import com.sics.rock.tableinsight4.internal.partitioner.FPrePartitionerTest;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FTempFile {

    private static final Logger logger = LoggerFactory.getLogger(FPrePartitionerTest.class);

    private static final File TEMP_DIR = new File("./tmp");

    private String name;

    private File path;

    public static FTempFile create(String content) {
        return create("TI4", content);
    }

    public static FTempFile create(String name, String content) {
        if (name.length() < 3) name = name + "_TI4";

        try {
            final FTempFile t = new FTempFile();
            final File file = File.createTempFile(name, ".tmp", TEMP_DIR);
            file.deleteOnExit();
            final BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file));
            os.write(content.getBytes(StandardCharsets.UTF_8));
            os.close();
            t.path = file;
            t.name = name;
            return t;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return name;
    }

    public File getPath() {
        return path;
    }

    static {
        logger.info("Init temp dir {}", TEMP_DIR.getAbsolutePath());
        if (TEMP_DIR.exists()) {
            try {
                FileUtils.deleteDirectory(TEMP_DIR);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (!TEMP_DIR.mkdir()) {
            throw new RuntimeException("Cannot make temp dir " + TEMP_DIR.getAbsolutePath());
        }

        TEMP_DIR.deleteOnExit();
    }
}
