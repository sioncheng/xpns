package com.github.sioncheng.xpns.server.akka;

import akka.actor.ActorRef;
import com.alibaba.fastjson.JSONObject;

public final class ClientActivation {

    public static final int LOGON = 1;
    public static final int LOGOUT = 2;
    public static final int HEARTBEAT = 3;
    public static final int ACK = 4;

    public static ClientActivation createLogon(ActorRef client, String acid, String version) {
        return create(client, acid, version, LOGON, null);
    }

    public static ClientActivation createLogout(ActorRef client, String acid, String version) {
        return create(client, acid, version, LOGOUT, null);
    }

    public static ClientActivation createHearbeat(ActorRef client, String acid, String version) {
        return create(client, acid, version, HEARTBEAT, null);
    }

    public static ClientActivation createAck(ActorRef client, String acid, String version, JSONObject jsonObject) {
        return create(client, acid, version, ACK, jsonObject);
    }

    public static ClientActivation create(ActorRef client, String acid, String version, int status, JSONObject jsonObject) {
        ClientActivation clientActivation = new ClientActivation();
        clientActivation.client = client;
        clientActivation.acid = acid;
        clientActivation.status = status;
        clientActivation.version = version;
        clientActivation.time = System.currentTimeMillis();
        clientActivation.jsonObject = jsonObject;

        return clientActivation;
    }

    public ActorRef client() {
        return client;
    }

    public String acid() {
        return acid;
    }

    public int status() {
        return status;
    }

    public String version () {return version;}

    public long time() {
        return time;
    }

    public JSONObject jsonObject() { return jsonObject;}

    private ActorRef client;
    private String acid;
    private int status;
    private String version;
    private long time;
    private JSONObject jsonObject;
}
