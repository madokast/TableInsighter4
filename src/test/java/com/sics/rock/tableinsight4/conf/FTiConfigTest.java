package com.sics.rock.tableinsight4.conf;

import com.sics.rock.tableinsight4.test.FTestTools;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FTiConfigTest extends FTestTools {

    @Test
    public void defaultConfig() {
        FTiConfig defaultConfig = FTiConfig.defaultConfig();
        defaultConfig.toConfigString().forEach(logger::info);
    }

    @Test
    public void load_default() {
        FTiConfig defaultConfig = FTiConfig.defaultConfig();
        List<String> confStrings = defaultConfig.toConfigString();

        FTiConfig load = FTiConfig.load(confStrings);
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load1() {
        // Expect a is Double
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.rule.cover=a"));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load2() {
        // Conf value is blank
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.rule.cover="));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load3() {
        // Conf key is blank
        FTiConfig load = FTiConfig.load(Collections.singletonList("=a"));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load4() {
        // Expect conf ti.rule.cover contain assign '='
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.rule.cover"));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load5() {
        // No conf matches ti.rule or rule
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.rule=1"));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load6() {
        // No conf matches ti
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti=1"));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load7() {
        // Cannot config a map value max : 10 on ti.rule.cover[Double]
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.rule.cover.max=10"));
        load.toConfigString().forEach(logger::info);
    }

    @Test
    public void load8() {
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.data.load.csv.options.a=a"));
        load.toConfigString().forEach(logger::info);
    }

    @Test
    public void load9() {
        FTiConfig load = FTiConfig.load(Collections.singletonList("ti.data.load.csv.options.header=123"));
        load.toConfigString().forEach(logger::info);
    }

    @Test
    public void load10() {
        FTiConfig load = FTiConfig.load(FUtils.listOf(
                "ti.data.load.csv.options.header=123",
                "ti.data.load.csv.options.header1=abc",
                "ti.data.load.csv.options.header2=1233"
        ));
        load.toConfigString().forEach(logger::info);
    }

    @Test(expected = FConfigParseException.class)
    public void load11() {
        // No conf matches ti.load.csv.options.
        FTiConfig load = FTiConfig.load(FUtils.listOf(
                "ti.data.load.csv.options.header=123",
                "ti.data.load.csv.options.=fsdfsd",
                "ti.data.load.csv.options.header2=1233"
        ));
        load.toConfigString().forEach(logger::info);
    }
}