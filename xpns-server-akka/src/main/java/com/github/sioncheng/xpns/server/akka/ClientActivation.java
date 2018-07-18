package com.github.sioncheng.xpns.server.akka;

import akka.actor.ActorRef;

public final class ClientActivation {

    public static final int LOGON = 1;
    public static final int LOGOUT = 2;

    public static ClientActivation createLogon(ActorRef client, String acid) {
        return create(client, acid, LOGON);
    }

    public static ClientActivation createLogout(ActorRef client, String acid) {
        return create(client, acid, LOGOUT);
    }

    public static ClientActivation create(ActorRef client, String acid, int status) {
        ClientActivation clientActivation = new ClientActivation();
        clientActivation.client = client;
        clientActivation.acid = acid;
        clientActivation.status = status;

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

    private ActorRef client;
    private String acid;
    private int status;
}
