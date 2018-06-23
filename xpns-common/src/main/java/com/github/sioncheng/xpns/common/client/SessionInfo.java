package com.github.sioncheng.xpns.common.client;

public class SessionInfo {

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    private String acid;

    private String server;
}