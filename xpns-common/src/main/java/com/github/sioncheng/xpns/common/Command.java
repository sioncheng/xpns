package com.github.sioncheng.xpns.common;

public class Command {

    public static final byte MAGIC_BYTE_HIGH = (byte)0xab;
    public static final byte MAGIC_BYTE_LOW = (byte)0x12;

    public static final byte HEARTBEAT = (byte)0x00;
    public static final byte REQUEST = (byte)0x01;
    public static final byte RESPONSE = (byte)0x02;

    public static final byte JSON_SERIALIZATION = (byte)0x01;


    public Command() {

    }


    private long serialNumber;
    private byte commandType;
    private byte serializationType;
    private int payloadLength;
    private byte[] payload;

}
