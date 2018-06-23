package com.github.sioncheng.xpns.common;

import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class CommandTest {

    @Test
    public void testMagicBytes() {
        byte b1 = Command.MAGIC_BYTE_HIGH;
        byte b2 = Command.MAGIC_BYTE_LOW;

        Assert.assertEquals(b1 & 0xff, 0xab);
        Assert.assertEquals(b2, 0x12);
    }


    @Test
    public void testCreate() throws Exception {
        final ConcurrentHashMap<Long, JsonCommand> jsonCommandConcurrentHashMap =
                new ConcurrentHashMap<Long, JsonCommand>();

        CountDownLatch countDownLatch = new CountDownLatch(10);

        for (int i = 0 ; i < 2; i++) {
            Thread thread1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 5; j++) {
                        JSONObject jsonObject = new JSONObject();
                        JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);
                        System.out.println(jsonCommand.getSerialNumber());
                        jsonCommandConcurrentHashMap.put(jsonCommand.getSerialNumber(), jsonCommand);

                        countDownLatch.countDown();
                    }
                }
            });

            thread1.start();
        }

        countDownLatch.await();

        Assert.assertEquals(10, jsonCommandConcurrentHashMap.size());


        for (int i = 1 ; i <= 10; i++) {
            JsonCommand jsonCommand = jsonCommandConcurrentHashMap.get(Long.valueOf(i));
            Assert.assertEquals(i, jsonCommand.getSerialNumber());
        }

    }

}
