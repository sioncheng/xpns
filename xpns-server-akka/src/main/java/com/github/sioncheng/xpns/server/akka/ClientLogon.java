package com.github.sioncheng.xpns.server.akka;

import akka.actor.ActorRef;

public final class ClientLogon {

    public static ClientLogon create(ActorRef client, String acid) {
        ClientLogon clientLogon = new ClientLogon();
        clientLogon.client = client;
        clientLogon.acid = acid;

        return clientLogon;
    }

    public ActorRef client() {
        return client;
    }

    public String acid() {
        return acid;
    }

    private ActorRef client;
    private String acid;
}
