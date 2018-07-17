package com.github.sioncheng.xpnsclient;

import com.github.sioncheng.xpns.common.console.CommandLineArgsReader;
import com.github.sioncheng.xpns.common.console.Prompt;

public class CommandArguments {

    public static CommandArguments readFromSystemIn() throws Exception {
        CommandArguments commandLineArgs = new CommandArguments();
        CommandLineArgsReader.fillFromSystemIn(commandLineArgs);
        return commandLineArgs;
    }

    public static CommandArguments readFromConfig(String config) throws Exception {
        CommandArguments commandLineArgs = new CommandArguments();
        CommandLineArgsReader.fillFromPropertiesConfig(commandLineArgs, config);
        return commandLineArgs;
    }


    public String getTargetHost() {
        return targetHost;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    public int getTargetPort() {
        return targetPort;
    }

    public void setTargetPort(int targetPort) {
        this.targetPort = targetPort;
    }

    public int getAppId() {
        return appId;
    }

    public void setAppId(int appId) {
        this.appId = appId;
    }

    public String getPrefixChar() {
        return prefixChar;
    }

    public void setPrefixChar(String prefixChar) {
        this.prefixChar = prefixChar;
    }

    public int getClientsNumber() {
        return clientsNumber;
    }

    public void setClientsNumber(int clientsNumber) {
        this.clientsNumber = clientsNumber;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    @Prompt(value = "target host:")
    private String targetHost;

    @Prompt(value = "target port:")
    private int targetPort;

    @Prompt(value = "client app id:")
    private int appId;

    @Prompt(value = "client id prefix char:")
    private String prefixChar;

    @Prompt(value = "clients number:")
    private int clientsNumber;

    @Prompt(value = "threads:")
    private int threads;
}
