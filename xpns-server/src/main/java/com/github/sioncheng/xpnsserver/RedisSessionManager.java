package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class RedisSessionManager implements SessionManager {

    public RedisSessionManager(String host, int port, int clientsMaxTotal, int clientsMaxIdle) {
        this.host = host;
        this.port = port;
        this.clientsMaxTotal = clientsMaxTotal;
        this.clientsMaxIdle = clientsMaxIdle;

        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxTotal(this.clientsMaxTotal);
        genericObjectPoolConfig.setMaxIdle(this.clientsMaxIdle);
        this.jedisPool = new JedisPool(genericObjectPoolConfig, host, port);
    }

    public void putClient(SessionInfo sessionInfo) {
        String jsonData = JSON.toJSONString(sessionInfo);
        String onlineKey = ONLINE + sessionInfo.getAcid();

        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();
            pipeline.set(onlineKey, jsonData);
            pipeline.expire(onlineKey, 3600);
            pipeline.sync();
        } catch (Exception ex) {
            logger.warn("put client error", ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void putClients(List<SessionInfo> sessionInfoList) {
        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            Pipeline pipeline = jedis.pipelined();

            for (SessionInfo sessionInfo :
                    sessionInfoList) {
                String jsonData = JSON.toJSONString(sessionInfo);
                String onlineKey = ONLINE + sessionInfo.getAcid();
                pipeline.set(onlineKey, jsonData);
                pipeline.expire(onlineKey, 3600);
            }

            pipeline.sync();
        } catch (Exception ex) {
            logger.warn("put client error", ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public void removeClient(String acid, String server) {
        SessionInfo sessionInfo = this.getClient(acid);
        if (sessionInfo != null && false == server.equals(sessionInfo.getServer())) {
            return;
        }

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.del(ONLINE + acid);
        } catch (Exception ex) {
            logger.warn("remove client error", ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public SessionInfo getClient(String acid) {
        Jedis jedis = null;
        String jsonData = null;
        try {
            jedis = jedisPool.getResource();
            jsonData = jedis.get(ONLINE + acid);
        } catch (Exception ex) {
            logger.warn("get client error", ex);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        if (StringUtils.isEmpty(jsonData)) {
            return null;
        } else {
            return JSON.parseObject(jsonData, SessionInfo.class);
        }
    }

    private String host;
    private int port;
    private int clientsMaxTotal;
    private int clientsMaxIdle;

    private JedisPool jedisPool;

    private static final String ONLINE = "ONLINE_";

    private static final Logger logger = LoggerFactory.getLogger(RedisSessionManager.class);
}
