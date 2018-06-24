package com.github.sioncheng.xpnsserver;

public class KafkaNotificationConsumerConfig {

    public KafkaNotificationConsumerConfig(){

    }

    public KafkaNotificationConsumerConfig(String bootstrapServer,
                                           String groupId,
                                           int sessionTimeoutMS,
                                           int autoCommitIntervalMS,
                                           boolean enableAutoCommit,
                                           String keyDeserializer,
                                           String valueDeserializer) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.sessionTimeoutMS = sessionTimeoutMS;
        this.autoCommitIntervalMS = autoCommitIntervalMS;
        this.enableAutoCommit = enableAutoCommit;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public int getSessionTimeoutMS() {
        return sessionTimeoutMS;
    }

    public void setSessionTimeoutMS(int sessionTimeoutMS) {
        this.sessionTimeoutMS = sessionTimeoutMS;
    }

    public int getAutoCommitIntervalMS() {
        return autoCommitIntervalMS;
    }

    public void setAutoCommitIntervalMS(int autoCommitIntervalMS) {
        this.autoCommitIntervalMS = autoCommitIntervalMS;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    private String bootstrapServer;
    private String groupId;
    private int sessionTimeoutMS;
    private int autoCommitIntervalMS;
    private boolean enableAutoCommit;
    private String keyDeserializer;
    private String valueDeserializer;
}
