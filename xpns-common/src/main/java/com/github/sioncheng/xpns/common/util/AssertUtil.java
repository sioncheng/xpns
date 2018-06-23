package com.github.sioncheng.xpns.common.util;

public class AssertUtil {

    public static void check(boolean b, String message) {
        if (!b) {
            throw new Error(message);
        }
    }
}
