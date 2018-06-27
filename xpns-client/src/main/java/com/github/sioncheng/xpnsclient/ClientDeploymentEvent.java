package com.github.sioncheng.xpnsclient;

public class ClientDeploymentEvent {

    public static final String EVENT_ADDRESS = "client.deployment";

    public ClientDeploymentEvent() {}

    public ClientDeploymentEvent(String acid, String verticleId) {
        this.acid = acid;
        this.verticleId = verticleId;
    }


    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public String getVerticleId() {
        return verticleId;
    }

    public void setVerticleId(String verticleId) {
        this.verticleId = verticleId;
    }

    private String acid;
    private String verticleId;
}
