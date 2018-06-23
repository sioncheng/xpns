package com.github.sioncheng.xpns.common.util;

public class SystemPropertyUtil {

    public static String getString(String propertyKey) {
        return System.getProperty(propertyKey);
    }

    public static int getInteger(String propertyKey) {
        return Integer.parseInt(System.getProperty(propertyKey));
    }

    public static boolean getBoolean(String propertyKey) {
        return Boolean.parseBoolean(System.getProperty(propertyKey));
    }
}
