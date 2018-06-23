package com.github.sioncheng.xpnsserver;

import com.github.sioncheng.xpns.common.Command;
import com.sun.tools.javac.util.Assert;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class CommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {

        while (true) {
            switch (status) {
                case EXPECT_MAGIC_BYTES :
                    if (byteBuf.readableBytes() < 2) {
                        byteBuf.markReaderIndex();
                        return;
                    }
                    Assert.check(Command.MAGIC_BYTE_HIGH == byteBuf.readByte(), "check magic byte high");
                    Assert.check(Command.MAGIC_BYTE_LOW == byteBuf.readByte(), "check magic byte low");

                    status = EXPECT_SERIAL_NUMBER;
                    break;
                case EXPECT_SERIAL_NUMBER:
                    if (byteBuf.readableBytes() < 8) {
                        byteBuf.markReaderIndex();
                        return;
                    }

                    byte[] serialNumberBytes = new byte[8];
                    byteBuf.readBytes(serialNumberBytes);
                    serialNumber = ByteBuffer.wrap(serialNumberBytes).order(ByteOrder.BIG_ENDIAN).getLong();

                    status = EXPECT_COMMAND_TYPE;
                    break;
                case EXPECT_COMMAND_TYPE:
                    if (byteBuf.readableBytes() < 1) {
                        byteBuf.markReaderIndex();
                        return;
                    }

                    commandType = byteBuf.readByte();

                    status = EXPECT_SERIALIZATION_TYPE;
                    break;
                case EXPECT_SERIALIZATION_TYPE:
                    if (byteBuf.readableBytes() < 1) {
                        byteBuf.markReaderIndex();
                        return;
                    }

                    serializationType = byteBuf.readByte();

                    status = EXPECT_PAYLOAD_LENGTH;
                    break;
                case EXPECT_PAYLOAD_LENGTH:
                    if (byteBuf.readableBytes() < 4) {
                        byteBuf.markReaderIndex();
                        return;
                    }

                    byte[] payloadLengthBytes = new byte[4];
                    byteBuf.readBytes(payloadLengthBytes);
                    payloadLength = ByteBuffer.wrap(payloadLengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                    Assert.check(payloadLength < 4096, "check payload length");

                    status = EXPECT_PAYLOAD;
                    break;
                case EXPECT_PAYLOAD:
                    if (byteBuf.readableBytes() < payloadLength) {
                        byteBuf.markReaderIndex();
                        return;
                    }

                    payloadBytes = new byte[payloadLength];
                    byteBuf.readBytes(payloadBytes);

                    Command command = new Command();
                    command.setSerialNumber(serialNumber);
                    command.setCommandType(commandType);
                    command.setSerializationType(serializationType);
                    command.setPayloadLength(payloadLength);
                    command.setPayloadBytes(payloadBytes);

                    list.add(command);

                    status = EXPECT_MAGIC_BYTES;
                    break;
                default:
                    break;
            }
        }
    }

    private int status = EXPECT_MAGIC_BYTES;

    private static final int EXPECT_MAGIC_BYTES = 1;
    private static final int EXPECT_SERIAL_NUMBER = 2;
    private static final int EXPECT_COMMAND_TYPE = 3;
    private static final int EXPECT_SERIALIZATION_TYPE = 4;
    private static final int EXPECT_PAYLOAD_LENGTH = 5;
    private static final int EXPECT_PAYLOAD = 6;

    private long serialNumber;
    private byte commandType;
    private byte serializationType;
    private int payloadLength;
    private byte[] payloadBytes;
}
