package com.github.sioncheng.xpns.common;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;

import java.util.Date;

public class DateFormatPatternsTest {

    @Test
    public void test() {
        String timeString = DateFormatUtils.format(new Date(),
                DateFormatPatterns.ISO8601_WITH_MS);

        System.out.println(timeString);

        String[] arr = new String[]{"1", "2"};
        String arrJsonString = JSON.toJSONString(arr);
        System.out.println(arrJsonString);

        String[] arr2 = JSON.parseObject(arrJsonString, (new String[]{}).getClass());
        for (String s:
             arr2) {
            System.out.println(s);
        }
    }
}
