package com.sics.rock.tableinsight4.conf;

import com.sics.rock.tableinsight4.utils.FTiUtils;
import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Configs in a TI task.
 *
 * @author zhaorx
 */
public class FTiConfig {

    private static final Logger logger = LoggerFactory.getLogger(FTiConfig.class);

    @FConfigItem(name = "ti.rule.cover", description = "Cover rate of rules")
    public Double cover = 0.05;

    @FConfigItem(name = "ti.rule.confidence", description = "Confidence rate of rules")
    public Double confidence = 0.8;

    @FConfigItem(name = "ti.rule.syntax.conjunction", description = "The symbol of logical operation conjunction in rule. e.g., ^, ⋀, &, &&")
    public String syntaxConjunction = "⋀";

    @FConfigItem(name = "ti.rule.syntax.implication", description = "The symbol of logical operation implication in rule. e.g. ->, =>, →")
    public String syntaxImplication = "->";

    @FConfigItem(name = "ti.rule.find.singleLine", description = "Do single-line rule finding of each table.")
    public Boolean singleLineRuleFind = true;

    @FConfigItem(name = "ti.rule.find.singleTableCrossLine", description = "Do single-table cross-line (2-line) rule finding of each table.")
    public Boolean singleTableCrossLineRuleFind = true;

    @FConfigItem(name = "ti.rule.find.crossTableCrossLine", description = "Do cross-table (2-line) cross-line (2-line) rule finding of each two tables.")
    public Boolean crossTableCrossLineRuleFind = true;

    @FConfigItem(name = "ti.rule.find.crossColumnThreshold", description = "A similarity threshold of two columns used as " +
            "a flag determining the construction of the corresponding cross-column predicate.")
    public Double crossColumnThreshold = 0.8;

    @FConfigItem(name = "ti.rule.find.constPredicateCrossLine", description = "Create constant predicates in cross line rule finding.")
    public Boolean constPredicateCrossLine = true;

    @FConfigItem(name = "ti.rule.maxLength", description = "The max number of predicates of a rule. The min value is 2 since the simplest rule is p1 -> p2 consisting of 2 predicates.")
    public Integer maxRuleLength = 20;

    @FConfigItem(name = "ti.rule.maxNumber", description = "The max number of rules in each rule find task. The rule finding will stop when number of rules beyond this maximum.")
    public Integer maxRuleNumber = 100_0000;

    @FConfigItem(name = "ti.rule.find.timeoutMinute", description = "The max processing time of each rule finding in minute.")
    public Integer ruleFindTimeoutMinute = 5;

    @FConfigItem(name = "ti.rule.find.maxNeedCreateChildrenRuleNumber", description = "The max number of rules that need create children. Keep the number under the limit preventing OOM.")
    public Integer maxNeedCreateChildrenRuleNumber = 1000_0000;

    @FConfigItem(name = "ti.rule.find.batchSizeMB", description = "The batch size of rules for validation in each loop, in MB.")
    public Integer ruleFindBatchSizeMB = 30;

    @FConfigItem(name = "ti.rule.positiveNegativeExample.switch", description = "The switch of positive/negative examples.")
    public Boolean positiveNegativeExampleSwitch = true;

    @FConfigItem(name = "ti.rule.positiveNegativeExample.number", description = "The number of positive and negative examples.")
    public Integer positiveNegativeExampleNumber = 10;

    @FConfigItem(name = "ti.internal.tableColumnLinker", description = "The linker string join table and column as identifier. Rename it only when the linker exists in column names. (e.g., __, @, __@@__)")
    public String tableColumnLinker = "@";

    @FConfigItem(name = "ti.data.load.orders", description = "If two or move ti.data.load.xxx.options are provided, attempt to load data in this order")
    public String tableLoadOrders = "JDBC,CSV,ORC";

    @FConfigItem(name = "ti.data.load.csv.options", description = "Options in csv file loaded by spark. See https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html")
    public Map<String, String> csvTableLoadOptions = FTiUtils.mapOf("header", "true", "inferSchema", "false");

    @FConfigItem(name = "ti.data.load.orc.options", description = "Options in orc file loaded by spark")
    public Map<String, String> orcTableLoadOptions = new HashMap<>();

    @FConfigItem(name = "ti.data.load.jdbc.options", description = "Options in table loaded by jdbc in spark")
    public Map<String, String> jdbcTableLoadOptions = FTiUtils.mapOf("url", "jdbc:", "user", "root", "password", "root");

    @FConfigItem(name = "ti.data.sample.sampleSwitch", description = "Table data sample switcher.")
    public Boolean sampleSwitch = false;

    @FConfigItem(name = "ti.data.sample.withReplacement", description = "Sample with replacement or not.")
    public Boolean sampleWithReplacement = false;

    @FConfigItem(name = "ti.data.sample.sampleRatio", description = "Sample ratio 0~1")
    public Double sampleRatio = 0.1;

    @FConfigItem(name = "ti.data.sample.sampleSeed", description = "Sample random seed.")
    public Long sampleSeed = 1234L;

    @FConfigItem(name = "ti.data.idColumnName", description = "ID column name. Used in identify positive/negative examples of rules")
    public String idColumnName = "row_id";

    @FConfigItem(name = "ti.derived.binaryModelDerivedColumnSuffix", description = "Derived column suffix for external binary model. Rename it only when conflicting with other column names.")
    public String externalBinaryModelDerivedColumnSuffix = "$EX_";

    @FConfigItem(name = "ti.derived.combineColumnLinker", description = "Derived combine columns linker. The linker should match its regex. Rename it only when conflicting with other column names.")
    public String combineColumnLinker = "||','||";

    @FConfigItem(name = "ti.derived.combineColumnLinkerRegex", description = "Derived combine columns linker regex. Rename it only when conflicting with other column names.")
    public String combineColumnLinkerRegex = "\\|\\|','\\|\\|";

    @FConfigItem(name = "ti.rule.constant.maxDecimalPlace", description = "The maximum number of decimal places reserved in rule output.")
    public Integer constantNumberMaxDecimalPlace = 2;

    @FConfigItem(name = "ti.rule.constant.allowExponentialForm", description = "Allow exponential form of decimal number in rule output.")
    public Boolean constantNumberAllowExponentialForm = true;

    @FConfigItem(name = "ti.rule.constant.findNullConstant", description = "The constants found includes null or not. The config can be overwritten in column-level")
    public Boolean findNullConstant = true;

    @FConfigItem(name = "ti.rule.constant.upperLimitRatio", description = "The upper limit of ratio of appear-time of constant value. The config can be overwritten in column-level")
    public Double constantUpperLimitRatio = 1.0;

    @FConfigItem(name = "ti.rule.constant.downLimitRatio", description = "The down limit of ratio of appear-time of constant value. The config can be overwritten in column-level")
    public Double constantDownLimitRatio = 0.1;

    @FConfigItem(name = "ti.rule.constant.interval.kMeans.kMeansIntervalSearch", description = "Flag searching intervals by k-means.")
    public Boolean kMeansIntervalSearch = true;

    @FConfigItem(name = "ti.rule.constant.interval.kMeans.clusterNumber", description = "Cluster number in k-means interval-constant finding. " +
            "The config can be overwritten in column-level")
    public Integer kMeansClusterNumber = 2;

    @FConfigItem(name = "ti.rule.constant.interval.kMeans.iterNumber", description = "Iteration number in k-means interval-constant finding. " +
            "The config can be overwritten in column-level")
    public Integer kMeansIterNumber = 1000;

    @FConfigItem(name = "ti.rule.constant.interval.leftClose", description = "The left boundary of interval-constant is close or not. " +
            "(a, b] is default, i.e. the left boundary is open (not close). The config can be overwritten in column-level." +
            "The config works on k-means algorithm and external interval info only.")
    public Boolean intervalLeftClose = false;

    @FConfigItem(name = "ti.rule.constant.interval.rightClose", description = "The right boundary of interval-constant is close or not. " +
            "(a, b] is default, i.e. the right boundary is close. The config can be overwritten in column-level." +
            "The config works on k-means algorithm and external interval info only.")
    public Boolean intervalRightClose = true;

    @FConfigItem(name = "ti.rule.constant.interval.usingConstantCreateInterval",
            description = "Flag using constants (dug by ratio or external info) to create intervals")
    public Boolean usingConstantCreateInterval = false;

    @FConfigItem(name = "ti.rule.constant.interval.constantIntervalOperators",
            description = "The operators used in intervals created by dug constants. Candidate operators are <, <=, > and >=." +
                    "For instance, if constant 30 and 40 dug from column 'age', " +
                    "then intervals 'age>=30', 'age<=30', 'age>=40' and 'age<=40' will be created " +
                    "if usingConstantCreateInterval is on and constantIntervalOperators is '>=,<='.")
    public String constantIntervalOperators = ">=,<=";

    @FConfigItem(name = "ti.rule.comparableColumnOperators",
            description = "The operators used in binary-predicate if the value-type of corresponding columns are comparable. " +
                    "Candidate operators are <, <=, > and >=. For instance, if the value-type of column 'age' is integer, " +
                    "then binary predicates t0.age>=t1.age and t0.age<=t1.age will be created " +
                    "if comparableColumnOperators '>=,<='. The default value is empty ' ' (blank space is required). " +
                    "Note, the binary-predicate with operator equal is created in any case.")
    public String comparableColumnOperators = " ";

    @FConfigItem(name = "ti.internal.sliceLengthForPLI", description = "Origin table splice length for PLI construction. " +
            "The value decide the PLI number and may affect the speed of ES construction. The recommended value may be 1000 ~ 2500. ")
    public Integer sliceLengthForPLI = 1000;

    @FConfigItem(name = "ti.internal.PLIBroadcastSizeMB", description = "The broadcast size in MByte. PLI will be broadcast in binary-line evidence set construction.")
    public Integer PLIBroadcastSizeMB = 1;

    @FConfigItem(name = "ti.internal.evidenceSet.partitionNumber", description = "The partition number of RDD which implements " +
            "the internal data structure evidence set. Number -1 means automatic determining.")
    public Integer evidenceSetPartitionNumber = -1;

    @FConfigItem(name = "ti.internal.randomSeed", description = "The random seed used in sampling.")
    public Long randomSeed = 1L;

    public static FTiConfig defaultConfig() {
        return new FTiConfig();
    }

    @SuppressWarnings("unchecked")
    public List<String> toConfigString() {
        List<String> configs = new ArrayList<>();
        try {
            for (Field f : FTiConfig.class.getDeclaredFields()) {
                if (f.isAnnotationPresent(FConfigItem.class)) {
                    FConfigItem annotation = f.getAnnotation(FConfigItem.class);
                    Class<?> type = f.getType();
                    Object val = f.get(this);
                    String key = annotation.name();
                    if (val != null && Map.class.isAssignableFrom(type)) {
                        ((Map<String, String>) val).forEach((k, v) -> {
                            configs.add(key + "." + k + "=" + v);
                        });
                    } else {
                        configs.add(key + "=" + val);
                    }
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        configs.sort(String::compareTo);
        return configs;
    }

    public static FTiConfig load(List<String> configs) {
        FTiConfig config = defaultConfig();

        Map<String, Field> keyFieldMap = new HashMap<>();
        for (Field f : FTiConfig.class.getDeclaredFields()) {
            if (f.isAnnotationPresent(FConfigItem.class)) {
                FConfigItem annotation = f.getAnnotation(FConfigItem.class);
                String key = annotation.name();
                keyFieldMap.put(key, f);
            }
        }

        for (String conf : configs) {
            if (StringUtils.isBlank(conf)) continue;
            try {
                int assign = conf.indexOf("=");
                if (assign == -1) throw new FConfigParseException("Expect conf " + conf + " contains assign '='");
                String key = conf.substring(0, assign);
                String val = conf.substring(assign + 1);
                if (StringUtils.isBlank(key)) throw new FConfigParseException("Conf key is blank");
                // blank is ok
                // if (StringUtils.isBlank(val)) throw new FConfigParseException("Conf value is blank");

                if (keyFieldMap.containsKey(key)) {// normal
                    Field field = keyFieldMap.get(key);
                    Class<?> type = field.getType();
                    Optional<?> castVal = FTypeUtils.cast(val, type);
                    if (castVal.isPresent()) {
                        field.set(config, castVal.get());
                    } else {
                        throw new FConfigParseException("Expect " + val + " is " + FTypeUtils.toString(type));
                    }
                } else {// map
                    int lastDot = key.lastIndexOf(".");
                    if (lastDot == -1) {
                        throw new FConfigParseException("No conf matches " + key);
                    }
                    String mainKey = key.substring(0, lastDot);
                    String subKey = key.substring(lastDot + 1);
                    loadMapConfig(config, keyFieldMap, val, mainKey, subKey);
                }
            } catch (Exception e) {
                throw new FConfigParseException("Cannot parse config " + conf, e);
            }
        }

        return config;
    }

    @SuppressWarnings("unchecked")
    private static void loadMapConfig(
            FTiConfig config, Map<String, Field> keyFieldMap,
            String val, String mainKey, String subKey) throws IllegalAccessException {
        if (StringUtils.isBlank(mainKey) ||
                StringUtils.isBlank(subKey) ||
                !keyFieldMap.containsKey(mainKey)) {
            final int lastDot = mainKey.lastIndexOf(".");
            if (lastDot == -1) throw new FConfigParseException("No conf matches " + mainKey + "." + subKey);
            final String nextMainKey = mainKey.substring(0, lastDot);
            final String nextSubKey = mainKey.substring(lastDot + 1) + "." + subKey;
            loadMapConfig(config, keyFieldMap, val, nextMainKey, nextSubKey);
        } else {
            Field field = keyFieldMap.get(mainKey);
            Class<?> type = field.getType();
            if (!Map.class.isAssignableFrom(type)) {
                throw new FConfigParseException("Cannot config a map value " + subKey + " : " + val
                        + " on " + mainKey + "[" + FTypeUtils.toString(type) + "]");
            }

            Map<String, String> map = (Map<String, String>) field.get(config);
            if (map == null) map = new HashMap<>();
            map.put(subKey, val);
            field.set(config, map);
        }
    }


    private FTiConfig() {
    }
}
