package com.github.sioncheng.xpnsserver.vertx;

public class RedisHelper {

    public static String generateOnlineKey(String acid) {
        return "VERTX_ONLINE_" + acid;
    }
}
