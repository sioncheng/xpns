package com.github.sioncheng.xpns.common;

import com.alibaba.fastjson.JSONObject;

import java.util.concurrent.atomic.AtomicLong;

public class JsonCommand {

    public static JsonCommand create(byte commandType, JSONObject commandObject) {
        return create(SerialNumberCounter.incrementAndGet(), commandType, commandObject);
    }

    public static JsonCommand create(long serialNumber, byte commandType, JSONObject commandObject) {
        JsonCommand jsonCommand = new JsonCommand();
        jsonCommand.serialNumber = serialNumber;
        jsonCommand.commandType = commandType;
        jsonCommand.commandObject = commandObject;

        return jsonCommand;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    public byte getCommandType() {
        return commandType;
    }

    public JSONObject getCommandObject() {
        return commandObject;
    }

    private long serialNumber;
    private byte commandType;
    private JSONObject commandObject;

    private static AtomicLong SerialNumberCounter = new AtomicLong(0);
}
