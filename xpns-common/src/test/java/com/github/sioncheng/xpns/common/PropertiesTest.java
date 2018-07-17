package com.github.sioncheng.xpns.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class PropertiesTest {

    @Test
    public void testLoadFromFileInputStream() throws Exception {
        Properties properties = new Properties();
        properties.load(PropertiesTest.class.getResourceAsStream("/config.properties"));

        Assert.assertEquals(2 , properties.size());
        for (Map.Entry<Object, Object> en :
                properties.entrySet()) {
            System.out.println(en.getKey() + " = " + en.getValue());
            System.out.println(en.getKey().getClass().getName());
            System.out.println(en.getValue().getClass().getName());
        }
    }
}
