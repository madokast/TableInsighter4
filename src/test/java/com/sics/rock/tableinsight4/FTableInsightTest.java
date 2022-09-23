package com.sics.rock.tableinsight4;

import com.sics.rock.tableinsight4.conf.FTiConfig;
import com.sics.rock.tableinsight4.rule.FRuleVO;
import com.sics.rock.tableinsight4.table.FColumnInfo;
import com.sics.rock.tableinsight4.table.FTableInfo;
import com.sics.rock.tableinsight4.table.column.FValueType;
import com.sics.rock.tableinsight4.test.FExamples;
import com.sics.rock.tableinsight4.test.FTableCreator;
import com.sics.rock.tableinsight4.test.env.FSparkEnv;
import com.sics.rock.tableinsight4.utils.FTiUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class FTableInsightTest extends FSparkEnv {

    @Test
    public void relation() {
        FTableInfo relation = FExamples.relation();
        FTiConfig config = FTiConfig.defaultConfig();
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
    public void cross_relation() {
        FTableInfo relation1 = FExamples.relation("r0", "t0");
        FTableInfo relation2 = FExamples.relation("r1", "t1");
        FTiConfig config = FTiConfig.defaultConfig();
        config.confidence = 0.8;
        config.cover = 0.001;
        config.singleTableCrossLineRuleFind = false;
        config.singleLineRuleFind = false;

        final FTableInsight TI = new FTableInsight(FTiUtils.listOf(relation1, relation2),
                Collections.emptyList(), config, spark);
        List<FRuleVO> rules = TI.findRule();

        for (final FRuleVO rule : rules) {
            logger.info("{}", rule);
        }

        logger.info("rules.size() = {}", rules.size());

        Assert.assertEquals(25, rules.size());
    }

    @Test
    public void test_number() {
        String tabName = "number";
        String header = "a1,a2";

        String[] contents = new String[]{
                "2,2",
                "2,2",
                "3,2"
        };

        File tab = FTableCreator.createCsv(header, contents);

        FTableInfo tableInfo = new FTableInfo(tabName, "tab01", tab.getAbsolutePath());
        for (String colName : header.split(",")) {
            tableInfo.addColumnInfo(new FColumnInfo(colName, FValueType.DOUBLE));
        }

        FTiConfig config = FTiConfig.defaultConfig();
        config.confidence = 0.0;
        config.cover = 0.001;
        config.usingConstantCreateInterval = true;
        config.comparableColumnOperators = ">=,<=";

        final FTableInsight TI = new FTableInsight(Collections.singletonList(tableInfo),
                Collections.emptyList(), config, spark);
        List<FRuleVO> rules = TI.findRule();

        for (final FRuleVO rule : rules) {
            logger.info("{}", rule);
        }

        Assert.assertTrue(rules.stream().map(FRuleVO::getRule).anyMatch(str -> str.contains(">=")));
        Assert.assertTrue(rules.stream().map(FRuleVO::getRule).anyMatch(str -> str.contains("<=")));

        logger.info("rules.size() = {}", rules.size());
    }
}