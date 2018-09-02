package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.client.SessionInfo;

import java.util.List;

public interface SessionManager {

    void putClient(SessionInfo sessionInfo);

    void putClients(List<SessionInfo> sessionInfoList);

    void removeClient(String acid, String server);

    SessionInfo getClient(String acid);
}
