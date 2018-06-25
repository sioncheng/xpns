package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSONObject;

public class ClientActivationEvent {

    public static final String EVENT_ADDRESS = "client.activation";

    public static final int LOGON_EVENT = 1;
    public static final int NOTIFICATION_EVENT = 2;
    public static final int CLOSE_EVENT = 3;

    public ClientActivationEvent() {}

    public ClientActivationEvent(String acid,  int eventType) {
        this.acid = acid;
        this.eventType = eventType;
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public JSONObject toJSONObject() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("acid", this.acid);
        jsonObject.put("eventType", this.eventType);

        return jsonObject;
    }

    public void fromJSONObject(JSONObject jsonObject) {
        this.setAcid(jsonObject.getString("acid"));
        this.setEventType(jsonObject.getInteger("eventType"));
    }

    private String acid;

    private int eventType;
}
