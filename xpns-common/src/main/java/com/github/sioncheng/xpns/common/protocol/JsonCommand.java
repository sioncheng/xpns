package com.github.sioncheng.xpns.common.protocol;

import com.alibaba.fastjson.JSONObject;

import java.util.concurrent.atomic.AtomicLong;

public class JsonCommand {

    private JsonCommand(){}

    public static final int LOGIN_CODE = 1;
    public static final int LOGON_CODE = 2;
    public static final int NOTIFICATION_CODE = 3;
    public static final int ACK_CODE = 4;
    public static final int HEART_BEAT_CODE = 5;

    public static final String COMMAND_CODE = "commandCode";
    public static final String ACID = "acid";
    public static final String NOTIFICATION = "notification";

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

    public int getCommandCode() {
        return this.commandObject.getInteger(COMMAND_CODE);
    }

    public JsonCommand clone() {
        JsonCommand jsonCommand = JsonCommand.create(this.getSerialNumber(),
                this.getCommandType(),
                this.getCommandObject());
        return jsonCommand;

    }

    public JsonCommand cloneWithCommandCode(int commandCode) {
        JsonCommand jsonCommand = this.clone();
        jsonCommand.commandObject.put(COMMAND_CODE, commandCode);
        return jsonCommand;
    }

    public String getAcid() {
        return this.commandObject.getString(ACID);
    }

    private long serialNumber;
    private byte commandType;
    private JSONObject commandObject;

    private static AtomicLong SerialNumberCounter = new AtomicLong(0);
}
