package com.github.sioncheng.xpns.bench.soa;

import com.github.sioncheng.xpns.common.console.CommandLineArgsReader;
import com.github.sioncheng.xpns.common.console.Prompt;

public class CommandArguments {

    public static CommandArguments readFromSystemIn() throws Exception {
        CommandArguments commandLineArgs = new CommandArguments();
        CommandLineArgsReader.fillFromSystemIn(commandLineArgs);
        return commandLineArgs;
    }

    public int getMessages() {
        return messages;
    }

    public void setMessages(int messages) {
        this.messages = messages;
    }

    public int getSeconds() {
        return seconds;
    }

    public void setSeconds(int seconds) {
        this.seconds = seconds;
    }

    public String getPrefixChars() {
        return prefixChars;
    }

    public void setPrefixChars(String prefixChars) {
        this.prefixChars = prefixChars;
    }

    public int getMaxClients() {
        return maxClients;
    }

    public void setMaxClients(int maxClients) {
        this.maxClients = maxClients;
    }

    public String getTargetHost() {
        return targetHost;
    }

    public void setTargetHost(String targetHost) {
        this.targetHost = targetHost;
    }

    @Prompt(value = "messages/per second:")
    private int messages;

    @Prompt(value = "seconds:")
    private int seconds;

    @Prompt(value = "prefix chars(a,b,c...):")
    private String prefixChars;

    @Prompt(value = "max clients/per prefix:")
    private int maxClients;

    @Prompt(value = "target host:")
    private String targetHost;
}
