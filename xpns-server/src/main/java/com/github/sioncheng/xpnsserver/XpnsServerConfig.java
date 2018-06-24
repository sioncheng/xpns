package com.github.sioncheng.xpnsserver;

public class XpnsServerConfig {

    public XpnsServerConfig() {

    }

    public XpnsServerConfig(int port,
                            int maxClients,
                            String apiServer,
                            int apiPort,
                            int nettyEventLoopGroupThreadsForClient,
                            int nettyEventLoopGroupThreadsForApi) {
        this.clientPort = port;
        this.maxClients = maxClients;
        this.apiServer = apiServer;
        this.apiPort = apiPort;
        this.nettyEventLoopGroupThreadsForClient = nettyEventLoopGroupThreadsForClient;
        this.nettyEventLoopGroupThreadsForApi = nettyEventLoopGroupThreadsForApi;
    }

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

    public int getNettyEventLoopGroupThreadsForClient() {
        return nettyEventLoopGroupThreadsForClient;
    }

    public void setNettyEventLoopGroupThreadsForClient(int nettyEventLoopGroupThreadsForClient) {
        this.nettyEventLoopGroupThreadsForClient = nettyEventLoopGroupThreadsForClient;
    }

    public int getApiPort() {
        return apiPort;
    }

    public void setApiPort(int apiPort) {
        this.apiPort = apiPort;
    }

    public int getNettyEventLoopGroupThreadsForApi() {
        return nettyEventLoopGroupThreadsForApi;
    }

    public void setNettyEventLoopGroupThreadsForApi(int nettyEventLoopGroupThreadsForApi) {
        this.nettyEventLoopGroupThreadsForApi = nettyEventLoopGroupThreadsForApi;
    }

    private int maxClients;
    private int clientPort;
    private String apiServer;
    private int apiPort;
    private int nettyEventLoopGroupThreadsForClient;
    private int nettyEventLoopGroupThreadsForApi;
}
