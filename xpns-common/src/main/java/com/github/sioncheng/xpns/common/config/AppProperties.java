package com.github.sioncheng.xpns.common.config;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class AppProperties {

    private static HashMap<String, String> properties = new HashMap<>();

    public static void init() throws  IOException {
        loadDefault();
        loadSpecific();
    }

    public static String getString(String key) {
        return properties.get(key);
    }

    public static int getInt(String key) {
        return Integer.parseInt(properties.get(key));
    }

    public static boolean getBoolean(String key) {
        return Boolean.parseBoolean(properties.get(key));
    }

    public static Map<String, String> getPropertiesWithPrefix(String keyPrefix) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, String> entry :
                properties.entrySet()) {
            String key = entry.getKey();
            if (StringUtils.startsWith(key, keyPrefix)) {
                result.put(key.replaceFirst(keyPrefix, ""), entry.getValue());
            }
        }

        return result;
    }


    public static Map<String, String> getPropertiesStartwith(String prefix) {
        Map<String, String> result = new HashMap<>();

        for (Map.Entry<String, String> entry :
                properties.entrySet()) {
            String key = entry.getKey();
            if (StringUtils.startsWith(key, prefix)) {
                result.put(key, entry.getValue());
            }
        }

        return result;
    }

    private static void loadDefault() throws IOException {
        InputStream inputStream =
                AppProperties.class.getClassLoader().getResourceAsStream("app.properties");

        loadFromFileConfig(inputStream);
    }

    private static void loadSpecific() throws IOException {
        String specific = System.getProperty("config");
        if (StringUtils.isEmpty(specific)) {
            return;
        }


        InputStream inputStream = new FileInputStream(specific);
        loadFromFileConfig(inputStream);
    }

    private static void loadFromFileConfig(InputStream inputStream) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);


        String line;
        while (null != (line = bufferedReader.readLine())) {
            line = StringUtils.trim(line);
            if (StringUtils.startsWith(line, "#")) {
                continue;
            }

            String[] tempArr = line.split("=");

            if (tempArr.length == 2) {
                properties.put(StringUtils.trim(tempArr[0]), StringUtils.trim(tempArr[1]));
            }
        }

        bufferedReader.close();
        inputStreamReader.close();
    }
}
