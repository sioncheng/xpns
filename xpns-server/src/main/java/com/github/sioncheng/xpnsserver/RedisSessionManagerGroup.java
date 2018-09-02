package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisSessionManagerGroup implements SessionManager {

    public RedisSessionManagerGroup(int instances, String host, int port,
                                    int clientsMaxTotal, int clientsMaxIdle) {
        this.stop = false;
        this.putClientQueues = new ArrayList<>(instances);
        this.redisSessionManagers = new ArrayList<>(instances);
        this.threads = new ArrayList<>(instances);

        for (int i = 0 ; i < instances; i++) {
            this.putClientQueues.add(new ConcurrentLinkedQueue<>());
            this.redisSessionManagers.add(new RedisSessionManager(host, port, clientsMaxTotal, clientsMaxIdle));
            final int n = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    threadWork(n);
                }
            });
            t.setName("redis-session-manager-group-thread-" + i);
            t.setDaemon(true);
            this.threads.add(t);
            t.start();
        }
    }

    public void shutdown() {
        this.stop = true;
    }

    public void putClient(SessionInfo sessionInfo) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("put client %s", sessionInfo.getAcid()));
        }

        String acid = sessionInfo.getAcid();

        this.putClientQueues.get(hashAcid(acid)).add(sessionInfo);
    }

    public void putClients(List<SessionInfo> sessionInfoList) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("put clients %d", sessionInfoList.size()));
        }

        for (SessionInfo sessionInfo :
                sessionInfoList) {
            this.putClient(sessionInfo);
        }

    }

    public void removeClient(String acid, String server) {
        getRedisSessionManager(acid).removeClient(acid, server);
    }

    public SessionInfo getClient(String acid) {
        return getRedisSessionManager(acid).getClient(acid);
    }

    private void threadWork(int n) {
        List<SessionInfo> sessionInfoList = new ArrayList<>();
        while (!stop) {

            SessionInfo sessionInfo = this.putClientQueues.get(n).poll();

            if (sessionInfo != null) {
                sessionInfoList.add(sessionInfo);
            }

            if (sessionInfoList.size() >= 100 ||
                    (sessionInfo == null && sessionInfoList.size() > 0)) {

                this.getRedisSessionManager(sessionInfoList.get(0).getAcid())
                        .putClients(sessionInfoList);

                sessionInfoList = new ArrayList<>();

                continue;
            }

            if (sessionInfo == null && sessionInfoList.size() == 0) {
                try {
                    Thread.sleep(100);
                } catch (Exception ex) {}
            }

        }
    }

    private RedisSessionManager getRedisSessionManager(String acid) {
        return redisSessionManagers.get(hashAcid(acid));
    }

    private int hashAcid(String acid) {
        return Math.abs(acid.charAt(acid.length() - 1)) % this.putClientQueues.size();
    }

    private List<ConcurrentLinkedQueue<SessionInfo>> putClientQueues;
    private List<RedisSessionManager> redisSessionManagers;
    private List<Thread> threads;

    private volatile boolean stop;

    private static final Logger logger = LoggerFactory.getLogger(RedisSessionManagerGroup.class);
}
