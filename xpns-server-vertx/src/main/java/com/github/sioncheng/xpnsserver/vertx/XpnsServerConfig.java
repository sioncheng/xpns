package com.github.sioncheng.xpnsserver.vertx;

public class XpnsServerConfig {


    public int getMaxClients() {
        return maxClients;
    }

    public void setMaxClients(int maxClients) {
        this.maxClients = maxClients;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }

    public String getApiHost() {
        return apiHost;
    }

    public void setApiHost(String apiHost) {
        this.apiHost = apiHost;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getClientInstances() {
        return clientInstances;
    }

    public void setClientInstances(int clientInstances) {
        this.clientInstances = clientInstances;
    }

    public int getApiInstances() {
        return apiInstances;
    }

    public void setApiInstances(int apiInstances) {
        this.apiInstances = apiInstances;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    private int maxClients;
    private int clientPort;
    private String apiHost;
    private int apiPort;
    private int workerThreads;
    private int clientInstances;
    private int apiInstances;
    private String redisHost;
    private int redisPort;
}
