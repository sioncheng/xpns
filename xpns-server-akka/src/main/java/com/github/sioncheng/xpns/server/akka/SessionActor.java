package com.github.sioncheng.xpns.server.akka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.function.Function;

public class SessionActor extends AbstractActor {

    public static Props props(String host, int port) {
        return Props.create(SessionActor.class,()->new SessionActor(host, port));
    }

    public SessionActor(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public void preStart() throws Exception {
        jedisPool = new JedisPool(host, port);
        super.preStart();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SessionEvent.class, msg -> handleSessionEvent(msg))
                .build();
    }

    private void handleSessionEvent(SessionEvent event) {
        switch (event.type()) {
            case SessionEvent.LOGON:
                setSessionInfo(event);
                break;
            case SessionEvent.LOGOUT:
                removeSessionInfo(event);
                break;
            case SessionEvent.HEARTBEAT:
                setSessionInfo(event);
                break;
            case SessionEvent.ASK:
                getSender().tell(getSessionInfo(event.acid()), getSelf());
                break;
        }
    }

    private void setSessionInfo(final SessionEvent event) {
        sessionOperation(x -> {
            SessionInfo sessionInfo = new SessionInfo();
            sessionInfo.setAcid(event.acid());
            sessionInfo.setServer(event.apiHost());
            sessionInfo.setPort(event.apiPort());

            String onlineKey = generateOnlineKey(event.acid());
            x.set(onlineKey, JSON.toJSONString(sessionInfo));
            x.expire(onlineKey, 3600);
            return null;
        });
    }

    private void removeSessionInfo(SessionEvent event) {

       sessionOperation(x -> {
           x.del(generateOnlineKey(event.acid()));
           return null;
       });

    }

    private SessionInfo getSessionInfo(String acid) {
        SessionInfo sessionInfo = null;
        Jedis jedis = jedisPool.getResource();
        try {
            String s = jedis.get(generateOnlineKey(acid));
            if (StringUtils.isNotEmpty(s)) {
                sessionInfo = JSON.parseObject(s, SessionInfo.class);
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }

        if (sessionInfo == null) {
            sessionInfo = new SessionInfo();
            sessionInfo.setAcid(acid);
            sessionInfo.setServer("");
            sessionInfo.setPort(0);
        }

        return sessionInfo;
    }

    private void sessionOperation(Function<Jedis,Void> action) {
        Jedis jedis = jedisPool.getResource();
        try {
            action.apply(jedis);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    private String generateOnlineKey(String acid) {
        return "ONLINE_" + acid;
    }


    private String host;
    private int port;
    JedisPool jedisPool;
}
