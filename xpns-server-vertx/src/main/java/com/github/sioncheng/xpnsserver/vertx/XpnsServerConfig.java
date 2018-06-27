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

    public String getApiServer() {
        return apiServer;
    }

    public void setApiServer(String apiServer) {
        this.apiServer = apiServer;
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

    private int maxClients;
    private int clientPort;
    private String apiServer;
    private int apiPort;
    private int workerThreads;
    private int clientInstances;
    private int apiInstances;
}
