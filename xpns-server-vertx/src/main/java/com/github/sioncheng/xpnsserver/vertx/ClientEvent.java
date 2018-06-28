package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSONObject;

public class ClientEvent {

    public static final String EVENT_ADDRESS_PREFIX = "client.event";

    public static final int START = 1;
    public static final int LOGON = 2;
    public static final int ACK = 3;
    public static final int HEART_BEAT = 4;
    public static final int STOP = 5;
    public static final int SOCKET_CLOSE = 6;
    public static final int LOGON_FOWARD = 7;

    public ClientEvent() {}

    public ClientEvent(int eventType,
                       String acid,
                       String deploymentId,
                       JSONObject commandObject) {
        this.eventType = eventType;
        this.acid = acid;
        this.deploymentId = deploymentId;
        this.commandObject = commandObject;
    }

    public ClientEvent clone() {
        ClientEvent clientEvent = new ClientEvent();
        clientEvent.setEventType(this.getEventType());
        clientEvent.setAcid(this.getAcid());
        clientEvent.setDeploymentId(this.getDeploymentId());
        clientEvent.setCommandObject(this.getCommandObject());

        return clientEvent;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public String getDeploymentId() {
        return deploymentId;
    }

    public void setDeploymentId(String deploymentId) {
        this.deploymentId = deploymentId;
    }

    public JSONObject getCommandObject() {
        return commandObject;
    }

    public void setCommandObject(JSONObject commandObject) {
        this.commandObject = commandObject;
    }

    private int eventType;

    private String acid;

    private String deploymentId;

    private JSONObject commandObject;

}
