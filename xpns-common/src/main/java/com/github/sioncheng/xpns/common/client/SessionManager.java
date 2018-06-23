package com.github.sioncheng.xpns.common.client;

public interface SessionManager {

    void putClient(String acid, String server);

    void removeClient(String acid, String server);

    SessionInfo getClient(String acid);
}
