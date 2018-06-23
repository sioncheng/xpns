package com.github.sioncheng.xpns.common.protocol;

public class Command {

    public static final byte MAGIC_BYTE_HIGH = (byte)0xab;
    public static final byte MAGIC_BYTE_LOW = (byte)0x12;

    public static final byte HEARTBEAT = (byte)0x00;
    public static final byte REQUEST = (byte)0x01;
    public static final byte RESPONSE = (byte)0x02;

    public static final byte JSON_SERIALIZATION = (byte)0x01;


    public Command() {

    }

    public Command(long serialNumber,
                   byte commandType,
                   byte serializationType,
                   int payloadLength,
                   byte[] payloadBytes) {
        this.serialNumber = serialNumber;
        this.commandType = commandType;
        this.serializationType = serializationType;
        this.payloadLength = payloadLength;
        this.payloadBytes = payloadBytes;
    }

    public long getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(long serialNumber) {
        this.serialNumber = serialNumber;
    }

    public byte getCommandType() {
        return commandType;
    }

    public void setCommandType(byte commandType) {
        this.commandType = commandType;
    }

    public byte getSerializationType() {
        return serializationType;
    }

    public void setSerializationType(byte serializationType) {
        this.serializationType = serializationType;
    }

    public int getPayloadLength() {
        return payloadLength;
    }

    public void setPayloadLength(int payloadLength) {
        this.payloadLength = payloadLength;
    }

    public byte[] getPayloadBytes() {
        return payloadBytes;
    }

    public void setPayloadBytes(byte[] payloadBytes) {
        this.payloadBytes = payloadBytes;
    }

    private long serialNumber;
    private byte commandType;
    private byte serializationType;
    private int payloadLength;
    private byte[] payloadBytes;

}
