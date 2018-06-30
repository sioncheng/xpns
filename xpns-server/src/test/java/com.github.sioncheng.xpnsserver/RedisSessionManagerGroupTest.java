package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.junit.Assert;
import org.junit.Test;

public class RedisSessionManagerGroupTest {

    @Test
    public void testPutGetRemove() throws Exception{
        RedisSessionManagerGroup redisSessionManagerGroup =
                new RedisSessionManagerGroup(2, "localhost", 6379);

        String acid = "32000001";
        String localhost = "localhost";

        SessionInfo sessionInfo = new SessionInfo();
        sessionInfo.setAcid(acid);
        sessionInfo.setServer(localhost);
        sessionInfo.setPort(9090);

        redisSessionManagerGroup.putClient(sessionInfo);
        Thread.sleep(500);

        SessionInfo sessionInfo1 = redisSessionManagerGroup.getClient(acid);
        Assert.assertNotNull(sessionInfo1);
        Assert.assertEquals(acid, sessionInfo1.getAcid());
        Assert.assertEquals(localhost, sessionInfo1.getServer());

        redisSessionManagerGroup.removeClient(acid, localhost);

        SessionInfo sessionInfo2 = redisSessionManagerGroup.getClient(acid);
        Assert.assertNull(sessionInfo2);
    }
}
