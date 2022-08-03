package com.sics.rock.tableinsight4.preprocessing.load;

import com.sics.rock.tableinsight4.conf.FConfigIllegalException;
import com.sics.rock.tableinsight4.env.FTiEnvironment;
import com.sics.rock.tableinsight4.preprocessing.load.impl.FCsvTableLoader;
import com.sics.rock.tableinsight4.preprocessing.load.impl.FJDBCTableLoader;
import com.sics.rock.tableinsight4.preprocessing.load.impl.FOrcTableLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaorx
 */
public class FTableLoader implements FITableLoader, FTiEnvironment {

    private static final Logger logger = LoggerFactory.getLogger(FTableLoader.class);

    private static final String JDBC = "JDBC";
    private static final String CSV = "CSV";
    private static final String ORC = "ORC";

    @Override
    public Dataset<Row> load(String tablePath) {

        Dataset<Row> dataset = null;

        for (String loader : config().tableLoadOrders.split(",")) {
            if (StringUtils.isBlank(loader)) continue;
            loader = loader.trim().toUpperCase();
            try {
                switch (loader) {
                    case JDBC:
                        if (FJDBCTableLoader.canLoad(tablePath, config().jdbcTableLoadOptions))
                            dataset = new FJDBCTableLoader(spark(), config().jdbcTableLoadOptions).load(tablePath);
                        break;
                    case CSV:
                        if (FCsvTableLoader.canLoad(tablePath, config().csvTableLoadOptions))
                            dataset = new FCsvTableLoader(spark(), config().csvTableLoadOptions).load(tablePath);
                        break;
                    case ORC:
                        if (FOrcTableLoader.canLoad(tablePath, config().orcTableLoadOptions))
                            dataset = new FOrcTableLoader(spark(), config().orcTableLoadOptions).load(tablePath);
                        break;
                    default:
                        throw new FConfigIllegalException("Unknown conf ti.data.load.orders " + loader);
                }
                if (dataset != null) break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (dataset != null) return dataset;

        logger.error("Can not load table from {}. Use empty table.", tablePath);
        return spark().emptyDataFrame();
    }
}
