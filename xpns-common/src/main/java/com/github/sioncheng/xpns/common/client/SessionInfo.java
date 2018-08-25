package com.github.sioncheng.xpns.common.client;

public class SessionInfo {

    public enum Type {

        LOGON(1), QUERY(2);

        private Type(int val) {
            this.val = val;
        }

        @Override
        public String toString() {
            if (this.val == 1) {
                return "LOGON";
            } else {
                return "QUERY";
            }
        }

        private int val;
    }

    public String getAcid() {
        return acid;
    }

    public void setAcid(String acid) {
        this.acid = acid;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private String acid;

    private String server;

    private int port;

    private Type type;

    private long timestamp;
}