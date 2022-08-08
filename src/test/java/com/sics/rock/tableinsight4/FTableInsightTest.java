package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.rule.FRule;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.test.env.FTableInsightEnv;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FTableInsightTest extends FSparkEnv {

    @Test
    public void findRule() {
        FTableInfo relation = FExamples.relation();
        FTiConfig config = new FTiConfig();
        List<FRule> rules = new FTableInsight(Collections.singletonList(relation),
                Collections.emptyList(), config, spark).findRule();

        logger.info("rules size = {}", rules.size());
    }
}