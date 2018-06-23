package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.junit.Assert;
import org.junit.Test;

public class RedisSessionManagerTest {

    @Test
    public void testPutGetRemove() {
        RedisSessionManager redisSessionManager = new RedisSessionManager("localhost", 6379);

        redisSessionManager.putClient("32000001", "localhost");

        SessionInfo sessionInfo = redisSessionManager.getClient("32000001");

        Assert.assertNotNull(sessionInfo);
        Assert.assertEquals("localhost", sessionInfo.getServer());
        Assert.assertEquals("32000001", sessionInfo.getAcid());

        redisSessionManager.removeClient("32000001", "localhost");

        SessionInfo sessionInfo1 = redisSessionManager.getClient("32000001");
        Assert.assertNull(sessionInfo1);
    }
}
