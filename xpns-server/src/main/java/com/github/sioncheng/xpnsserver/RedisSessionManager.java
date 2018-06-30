package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisSessionManager implements SessionManager {

    public RedisSessionManager(String host, int port) {
        this.host = host;
        this.port = port;

        this.jedisPool = new JedisPool(host, port);
    }

    public void putClient(SessionInfo sessionInfo) {
        String jsonData = JSON.toJSONString(sessionInfo);
        String onlineKey = ONLINE + sessionInfo.getAcid();

        Jedis jedis = jedisPool.getResource();

        Pipeline pipeline = jedis.pipelined();
        pipeline.set(onlineKey, jsonData);
        pipeline.expire(onlineKey, 3600);
        pipeline.sync();

        jedis.close();
    }

    public void removeClient(String acid, String server) {
        SessionInfo sessionInfo = this.getClient(acid);
        if (sessionInfo != null && false == server.equals(sessionInfo.getServer())) {
            return;
        }

        Jedis jedis = jedisPool.getResource();
        jedis.del(ONLINE + acid);
        jedis.close();
    }

    public SessionInfo getClient(String acid) {
        Jedis jedis = jedisPool.getResource();
        String jsonData = jedis.get(ONLINE + acid);
        jedis.close();
        if (StringUtils.isEmpty(jsonData)) {
            return null;
        } else {
            return JSON.parseObject(jsonData, SessionInfo.class);
        }
    }

    private String host;
    private int port;

    private JedisPool jedisPool;

    private static final String ONLINE = "ONLINE_";
}
