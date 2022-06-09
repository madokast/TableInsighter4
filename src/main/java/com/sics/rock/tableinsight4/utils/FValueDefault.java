package com.sics.rock.tableinsight4.utils;

import java.util.Optional;

/**
 * return value is null ? default : value
 *
 * @author zhaorx
 */
public class FValueDefault {

    public static <T> T getOrDefault(T value, T defaultVal) {
        FAssertUtils.require(defaultVal != null, "defaultVal should not be null");
        return value == null ? defaultVal : value;
    }

    public static boolean getOrDefault(Boolean value, boolean defaultVal) {
        return value == null ? defaultVal : value;
    }

    public static int getOrDefault(Integer value, int defaultVal) {
        return value == null ? defaultVal : value;
    }

    public static double getOrDefault(Double value, double defaultVal) {
        return value == null ? defaultVal : value;
    }


}
