package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;

public interface SessionManager {

    void putClient(SessionInfo sessionInfo);

    void removeClient(String acid, String server);

    SessionInfo getClient(String acid);
}
