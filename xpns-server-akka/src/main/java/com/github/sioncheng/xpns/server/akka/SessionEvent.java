package com.github.sioncheng.xpns.server.akka;

public class SessionEvent {

    public static final int LOGON = 1;
    public static final int LOGOUT = 2;
    public static final int HEARTBEAT = 3;
    public static final int ASK = 4;

    public static SessionEvent createAsk(String acid) {
        return create(acid, ASK, "",0);
    }

    public static SessionEvent create(String acid, int type, String apiHost, int apiPort) {
        SessionEvent sessionEvent = new SessionEvent();
        sessionEvent.acid = acid;
        sessionEvent.type = type;
        sessionEvent.apiHost = apiHost;
        sessionEvent.apiPort = apiPort;

        return sessionEvent;
    }

    public String acid() {
        return acid;
    }

    public int type() {
        return type;
    }

    public String apiHost() {
        return apiHost;
    }

    public int apiPort() {
        return apiPort;
    }

    private String acid;
    private int type;
    private String apiHost;
    private int apiPort;
}
