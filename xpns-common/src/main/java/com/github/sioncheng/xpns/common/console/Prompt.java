package com.github.sioncheng.xpns.common.console;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author : cyq
 * @date : 2018/5/30 2:41 PM
 * Description:
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Prompt {

    String value() default "";
}
