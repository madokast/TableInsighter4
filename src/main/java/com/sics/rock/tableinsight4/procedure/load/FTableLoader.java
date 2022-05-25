package com.sics.rock.tableinsight4.procedure.load;

import com.sics.rock.tableinsight4.FTiEnvironment;
import com.sics.rock.tableinsight4.procedure.load.impl.FCsvTableLoader;
import com.sics.rock.tableinsight4.procedure.load.impl.FJDBCTableLoader;
import com.sics.rock.tableinsight4.procedure.load.impl.FOrcTableLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FTableLoader implements FITableLoader, FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FTableLoader.class);

    private static final String JDBC = "JDBC";
    private static final String CSV = "CSV";
    private static final String ORC = "ORC";

    private static final Map<String, Class<? extends FITableLoader>> TABLE_LOADER_MAP = new HashMap<>();

    private final Map<String, Map<String, String>> optionsMap = new HashMap<>();

    @Override
    public Dataset<Row> load(String tablePath) {

        for (String loader : config().tableLoadOrders.split(",")) {
            loader = loader.trim().toUpperCase();
            final Class<? extends FITableLoader> tl = TABLE_LOADER_MAP.getOrDefault(loader, null);
            if (tl == null) {
                logger.warn("Unknown table data loader {}", loader);
                continue;
            }
            try {
                final Method canLoad = tl.getMethod("canLoad", String.class, Map.class);
                final Object r = canLoad.invoke(null, tablePath, optionsMap.getOrDefault(loader, Collections.emptyMap()));
                if (r != null && Boolean.parseBoolean(r.toString())) {
                    final Constructor<? extends FITableLoader> cons = tl.getConstructor(SparkSession.class, Map.class);
                    final FITableLoader instance = cons.newInstance(spark(), optionsMap.getOrDefault(loader, Collections.emptyMap()));
                    return instance.load(tablePath);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        logger.error("Can not load table from {}. Use empty table.", tablePath);
        return spark().emptyDataFrame();
    }

    public FTableLoader() {
        optionsMap.put(JDBC, config().jdbcTableLoadOptions);
        optionsMap.put(ORC, config().orcTableLoadOptions);
        optionsMap.put(CSV, config().csvTableLoadOptions);
    }

    static {
        TABLE_LOADER_MAP.put(JDBC, FJDBCTableLoader.class);
        TABLE_LOADER_MAP.put(ORC, FOrcTableLoader.class);
        TABLE_LOADER_MAP.put(CSV, FCsvTableLoader.class);
    }
}
