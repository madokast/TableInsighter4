package com.sics.rock.tableinsight4.preprocessing;

import com.sics.rock.tableinsight4.preprocessing.load.FTableLoader;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class FDatasetSamplerTest extends FTableInsightEnv {

    @Test
    public void sample() {

        config().sampleSwitch = true;

        final FTableInfo relation = FExamples.relation();

        final Dataset<Row> dataset = new FTableLoader().load(relation.getTableDataPath());

        dataset.show();

        final FDatasetSampler sampler = new FDatasetSampler(config().sampleSwitch,
                config().sampleWithReplacement, config().samplingRandomSeed, config().sampleRatio);

        sampler.sample(dataset, relation.getTableName()).show();
    }
}