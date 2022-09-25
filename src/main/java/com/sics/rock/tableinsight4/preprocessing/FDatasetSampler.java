package com.sics.rock.tableinsight4.preprocessing;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FDatasetSampler {

    private static final Logger logger = LoggerFactory.getLogger(FDatasetSampler.class);

    private final boolean sample;

    private final boolean withReplacement;

    private final long seed;

    private final double ratio;

    public <E> Dataset<E> sample(Dataset<E> originData, String tableName) {
        if (sample) {
            logger.info("Sample {} with ratio {}", tableName, ratio);
            return originData.sample(withReplacement, ratio, seed);
        } else {
            return originData;
        }
    }

    public FDatasetSampler(final boolean sample, final boolean withReplacement, final long seed, final double ratio) {
        this.sample = sample;
        this.withReplacement = withReplacement;
        this.seed = seed;
        this.ratio = ratio;
    }
}
