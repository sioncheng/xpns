package com.github.sioncheng.xpns.common.console;

import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * @author : cyq
 * @date : 2018/5/30 2:33 PM
 * Description:
 */
public class CommandLineArgsReader {

    public static void fillFromSystemIn(Object o) throws Exception {

        Scanner scanner = new Scanner(System.in);

        try {
            Class clazz = o.getClass();
            Field[] fields = clazz.getDeclaredFields();

            for (Field field :
                    fields) {

                field.setAccessible(true);

                if (field.isAnnotationPresent(Prompt.class)) {
                    Prompt prompt = field.getAnnotation(Prompt.class);
                    System.out.print(prompt.value());
                    String value = scanner.nextLine();


                    Class ft = field.getType();
                    if (ft == Integer.class || ft.getName().equalsIgnoreCase("int")) {
                        field.set(o, Integer.parseInt(value));
                    } else if (ft == String.class) {
                        field.set(o, value);
                    } else {
                        System.out.print(String.format("not support %s yet", ft.getName()));
                    }
                }
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            //scanner.close();
        }
    }


    public static void fillFromPropertiesConfig(Object o, String filepath)
            throws IOException, IllegalAccessException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(filepath));


        Class clazz = o.getClass();
        Field[] fields = clazz.getDeclaredFields();

        for (Field field :
                fields) {

            field.setAccessible(true);

            if (field.isAnnotationPresent(Prompt.class)) {
                Prompt prompt = field.getAnnotation(Prompt.class);
                Object value = null;
                for (Map.Entry<Object, Object> entry:
                     properties.entrySet()) {
                    if (prompt.value().replace(" ","_" ).startsWith((String)entry.getKey())) {
                        value = entry.getValue();
                        break;
                    }
                }


                Class ft = field.getType();
                if (ft == Integer.class || ft.getName().equalsIgnoreCase("int")) {
                    field.set(o, Integer.parseInt((String)value));
                } else if (ft == String.class) {
                    field.set(o, value);
                } else {
                    System.out.print(String.format("not support %s yet", ft.getName()));
                }
            }
        }
    }


    public static void refillFromSystemD(Object o) throws IllegalAccessException {


        Class clazz = o.getClass();
        Field[] fields = clazz.getDeclaredFields();

        for (Field field :
                fields) {

            field.setAccessible(true);

            if (field.isAnnotationPresent(Prompt.class)) {
                Prompt prompt = field.getAnnotation(Prompt.class);
                String key = prompt.value().replace(" ", "_").replace(":","");
                String value = System.getProperty(key);
                if (StringUtils.isEmpty(value)) {
                    continue;
                }


                Class ft = field.getType();
                if (ft == Integer.class || ft.getName().equalsIgnoreCase("int")) {
                    field.set(o, Integer.parseInt(value));
                } else if (ft == String.class) {
                    field.set(o, value);
                } else {
                    System.out.print(String.format("not support %s yet", ft.getName()));
                }
            }
        }
    }
}
