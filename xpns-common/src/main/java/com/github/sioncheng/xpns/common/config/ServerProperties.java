package com.github.sioncheng.xpns.common.config;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;

public class ServerProperties {

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

    private static void loadDefault() throws IOException {
        InputStream inputStream =
                ServerProperties.class.getClassLoader().getResourceAsStream("xpns-server.properties");

        loadFromFileConfig(inputStream);
    }

    private static void loadSpecific() throws IOException {
        String specific = System.getProperty("xpns-server-config");
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
            String[] tempArr = StringUtils.trim(line).split("=");

            if (tempArr.length == 2) {
                properties.put(StringUtils.trim(tempArr[0]), StringUtils.trim(tempArr[1]));
            }
        }

        bufferedReader.close();
        inputStreamReader.close();
    }
}
