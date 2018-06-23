package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.protocol.Command;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CommandEncoder extends MessageToByteEncoder {

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Object o, ByteBuf byteBuf) {

        Command command = (Command)o;

        byteBuf.writeByte(Command.MAGIC_BYTE_HIGH);
        byteBuf.writeByte(Command.MAGIC_BYTE_LOW);

        byte[] serialNumberBytes = new byte[8];
        ByteBuffer.wrap(serialNumberBytes).order(ByteOrder.BIG_ENDIAN).putLong(command.getSerialNumber());
        byteBuf.writeBytes(serialNumberBytes);

        byteBuf.writeByte(command.getCommandType());

        byteBuf.writeByte(command.getSerializationType());

        byte[] payloadLengthBytes = new byte[4];
        ByteBuffer.wrap(payloadLengthBytes).order(ByteOrder.BIG_ENDIAN).putInt(command.getPayloadLength());
        byteBuf.writeBytes(payloadLengthBytes);

        byteBuf.writeBytes(command.getPayloadBytes());
    }
}
