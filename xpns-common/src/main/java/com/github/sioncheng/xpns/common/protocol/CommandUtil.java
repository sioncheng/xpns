package com.github.sioncheng.xpns.common.protocol;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CommandUtil {


    public static byte[] serializeCommand(Command command) {
        byte[] totalBytes = new byte[16 + command.getPayloadLength()];
        totalBytes[0] = Command.MAGIC_BYTE_HIGH;
        totalBytes[1] = Command.MAGIC_BYTE_LOW;
        ByteBuffer.wrap(totalBytes, 2, 8).order(ByteOrder.BIG_ENDIAN).putLong(command.getSerialNumber());
        totalBytes[10] = command.getCommandType();
        totalBytes[11] = command.getSerializationType();
        ByteBuffer.wrap(totalBytes,12,4).order(ByteOrder.BIG_ENDIAN).putInt(command.getPayloadLength());
        for (int i = 0; i < command.getPayloadLength(); i++) {
            totalBytes[16 + i] = command.getPayloadBytes()[i];
        }


        return totalBytes;
    }
}
