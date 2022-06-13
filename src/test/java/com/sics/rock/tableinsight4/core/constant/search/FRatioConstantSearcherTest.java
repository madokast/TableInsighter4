package com.sics.rock.tableinsight4.core.constant.search;

import com.sics.rock.tableinsight4.core.FTableDataLoader;
import com.sics.rock.tableinsight4.core.constant.FConstantInfo;
import com.sics.rock.tableinsight4.table.FTableDatasetMap;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class FRatioConstantSearcherTest extends FTableInsightEnv {

    @Test
    public void test() {
        final FTableInfo relation = FExamples.relation();
        final FTableDataLoader loader = new FTableDataLoader();
        final FTableDatasetMap tableDatasetMap = loader.prepareData(Collections.singletonList(relation));
        final FRatioConstantSearcher searcher = new FRatioConstantSearcher(
                config().findNullConstant, config().constantUpperLimitRatio, config().constantDownLimitRatio
        );
        for (FConstantInfo c : searcher.search(tableDatasetMap)) {
            logger.info("Find {}", c);
        }
    }

}