package com.sics.rock.tableinsight4.conf;

import com.sics.rock.tableinsight4.utils.FTypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Config with default value
 * see its implemented subset
 *
 * @author zhaorx
 */
public abstract class FTiConfiguring implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(FTiConfiguring.class);

    private final Map<String, Object> otherConfigs = new HashMap<>();

    public void config(String key, Object value) {
        this.otherConfigs.put(key, value);
    }

    public <Value> Value getConfig(String key, Value defaultValue) {
        Object ret = null;
        for (String _k : otherConfigs.keySet()) {
            if (key.contains(_k) || _k.contains(key)) {
                if (ret == null) {
                    ret = otherConfigs.get(_k);
                } else {
                    logger.warn("Ambiguous config key {} matches {}", key, otherConfigs);
                    if (logger.isDebugEnabled()) {
                        throw new RuntimeException("Ambiguous config key " + key + " matches " + otherConfigs);
                    }
                }
            }
        }

        if (ret == null) {
            return defaultValue;
        } else {
            Optional<Value> cast = FTypeUtils.castAs(ret, defaultValue);
            if (cast.isPresent()) {
                return cast.get();
            } else {
                logger.warn("Cannot read config {}: {}. Use upper value {}", key, ret, defaultValue);
                if (logger.isDebugEnabled()) {
                    throw new RuntimeException("Cannot read config " + key + ": " + ret);
                }
                return defaultValue;
            }
        }
    }
}
