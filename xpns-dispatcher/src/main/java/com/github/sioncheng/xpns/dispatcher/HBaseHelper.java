package com.github.sioncheng.xpns.dispatcher;

import org.apache.hadoop.hbase.client.Result;

import java.io.UnsupportedEncodingException;

public class HBaseHelper {

    public static String getValue(Result result, byte[] family, byte[] qualifier) throws UnsupportedEncodingException {
        return new String(result.getValue(family, qualifier), "UTF-8");
    }
}
