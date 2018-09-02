package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.junit.Assert;
import org.junit.Test;

public class RedisSessionManagerTest {

    @Test
    public void testPutGetRemove() {
        RedisSessionManager redisSessionManager = new RedisSessionManager("localhost", 6379,4,4);

        SessionInfo sessionInfo = new SessionInfo();
        sessionInfo.setAcid("32000001");
        sessionInfo.setServer("localhost");
        sessionInfo.setPort(9090);
        redisSessionManager.putClient(sessionInfo);

        SessionInfo sessionInfo1 = redisSessionManager.getClient("32000001");

        Assert.assertNotNull(sessionInfo1);
        Assert.assertEquals("localhost", sessionInfo1.getServer());
        Assert.assertEquals("32000001", sessionInfo1.getAcid());

        redisSessionManager.removeClient("32000001", "localhost");

        SessionInfo sessionInfo2 = redisSessionManager.getClient("32000001");
        Assert.assertNull(sessionInfo2);
    }
}
