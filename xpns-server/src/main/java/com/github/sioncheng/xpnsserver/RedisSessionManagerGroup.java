package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RedisSessionManagerGroup implements SessionManager {

    public RedisSessionManagerGroup(int instances, String host, int port) {
        this.stop = false;
        this.putClientQueues = new ArrayList<>(instances);
        this.redisSessionManagers = new ArrayList<>(instances);
        this.threads = new ArrayList<>(instances);

        for (int i = 0 ; i < instances; i++) {
            this.putClientQueues.add(new ConcurrentLinkedQueue<>());
            this.redisSessionManagers.add(new RedisSessionManager(host, port));
            final int n = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    threadWork(n);
                }
            });
            this.threads.add(t);
            t.start();
        }
    }

    public void shutdown() {
        this.stop = true;
    }

    public void putClient(SessionInfo sessionInfo) {
        String acid = sessionInfo.getAcid();

        this.putClientQueues.get(hashAcid(acid)).add(sessionInfo);
    }

    public void removeClient(String acid, String server) {
        getRedisSessionManager(acid).removeClient(acid, server);
    }

    public SessionInfo getClient(String acid) {
        return getRedisSessionManager(acid).getClient(acid);
    }

    private void threadWork(int n) {
        while (!stop) {
            SessionInfo sessionInfo = this.putClientQueues.get(n).poll();
            if (sessionInfo == null) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) { }

                continue;
            }

            this.getRedisSessionManager(sessionInfo.getAcid())
                    .putClient(sessionInfo);
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
}
