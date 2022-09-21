package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.rule.FRuleVO;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FTableInsightTest extends FSparkEnv {

    @Test
    public void find() {
        FTableInfo relation = FExamples.relation();
        FTiConfig config = new FTiConfig();
        config.confidence = 0.8;
        config.cover = 0.001;

        final FTableInsight TI = new FTableInsight(Collections.singletonList(relation),
                Collections.emptyList(), config, spark);
        List<FRuleVO> rules = TI.findRule();

        for (final FRuleVO rule : rules) {
            logger.info("{}", rule);
        }

        Assert.assertEquals(400, rules.size());
    }

    @Test
    public void cross_table() {
        FTableInfo relation1 = FExamples.relation("r0", "t0");
        FTableInfo relation2 = FExamples.relation("r1", "t1");
        FTiConfig config = new FTiConfig();
        config.confidence = 0.8;
        config.cover = 0.001;
        config.singleTableCrossLineRuleFind=false;
        config.singleLineRuleFind=false;

        final FTableInsight TI = new FTableInsight(FTiUtils.listOf(relation1, relation2),
                Collections.emptyList(), config, spark);
        List<FRuleVO> rules = TI.findRule();

        for (final FRuleVO rule : rules) {
            logger.info("{}", rule);
        }

        logger.info("rules.size() = {}", rules.size());

        Assert.assertEquals(25, rules.size());
    }
}