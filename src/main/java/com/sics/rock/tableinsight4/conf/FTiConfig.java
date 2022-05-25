package com.sics.rock.tableinsight4.conf;

import com.sics.rock.tableinsight4.utils.FTypeUtils;
import com.sics.rock.tableinsight4.utils.FUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * Configs in a task.
 *
 * @author zhaorx
 */
public class FTiConfig {

    private static final Logger logger = LoggerFactory.getLogger(FTiConfig.class);

    @FConfigItemAnnotation(name = "ti.rule.cover", description = "Cover rate of rules")
    public Double cover = 0.05;

    @FConfigItemAnnotation(name = "ti.rule.confidence", description = "Confidence rate of rules")
    public Double confidence = 0.8;

    @FConfigItemAnnotation(name = "ti.data.load.orders", description = "If two or move ti.data.load.xxx.options are provided, attempt to load data in this order")
    public String tableLoadOrders = "JDBC,ORC,CSV";

    @FConfigItemAnnotation(name = "ti.data.load.csv.options", description = "Options in csv file load by spark. See https://spark.apache.org/docs/3.2.0/sql-data-sources-csv.html")
    public Map<String, String> csvTableLoadOptions = FUtils.mapOf("header", "true", "inferSchema", "false");

    @FConfigItemAnnotation(name = "ti.data.load.orc.options", description = "Options in orc file load by spark")
    public Map<String, String> orcTableLoadOptions = FUtils.mapOf("spark.sql.orc.impl", "native");

    @FConfigItemAnnotation(name = "ti.data.load.jdbc.options", description = "Options in table load by jdbc in spark")
    public Map<String, String> jdbcTableLoadOptions = FUtils.mapOf("url", "jdbc:", "user", "root", "password", "root");

    @FConfigItemAnnotation(name = "ti.data.idColumnName", description = "ID column name. Used in identify positive/negative examples of rules")
    public String idColumnName = "row_id";


    public static FTiConfig defaultConfig() {
        return new FTiConfig();
    }

    @SuppressWarnings("unchecked")
    public List<String> toConfigString() {
        List<String> configs = new ArrayList<>();
        try {
            for (Field f : FTiConfig.class.getDeclaredFields()) {
                if (f.isAnnotationPresent(FConfigItemAnnotation.class)) {
                    FConfigItemAnnotation annotation = f.getAnnotation(FConfigItemAnnotation.class);
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
            if (f.isAnnotationPresent(FConfigItemAnnotation.class)) {
                FConfigItemAnnotation annotation = f.getAnnotation(FConfigItemAnnotation.class);
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
                if (StringUtils.isBlank(val)) throw new FConfigParseException("Conf value is blank");

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
                    // TODO 递归
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
    private static void loadMapConfig(FTiConfig config, Map<String, Field> keyFieldMap,
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

}