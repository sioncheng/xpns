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

        redisSessionManagerGroup.putClient(acid, localhost);
        Thread.sleep(500);

        SessionInfo sessionInfo = redisSessionManagerGroup.getClient(acid);
        Assert.assertNotNull(sessionInfo);
        Assert.assertEquals(acid, sessionInfo.getAcid());
        Assert.assertEquals(localhost, sessionInfo.getServer());

        redisSessionManagerGroup.removeClient(acid, localhost);

        SessionInfo sessionInfo1 = redisSessionManagerGroup.getClient(acid);
        Assert.assertNull(sessionInfo1);
    }
}
