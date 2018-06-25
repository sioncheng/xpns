package com.github.sioncheng.xpns.common.console;

import java.lang.reflect.Field;
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
}
