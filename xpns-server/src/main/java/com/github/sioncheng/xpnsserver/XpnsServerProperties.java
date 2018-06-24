package com.github.sioncheng.xpnsserver;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;

public class XpnsServerProperties {

    private static HashMap<String, String> properties = new HashMap<>();

    public static void init() throws  IOException {
        loadDefault();
        loadSpecific();
    }

    private static void loadDefault() throws IOException {
        InputStream inputStream =
                XpnsServerProperties.class.getClassLoader().getResourceAsStream("xpns-server.properties");

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
