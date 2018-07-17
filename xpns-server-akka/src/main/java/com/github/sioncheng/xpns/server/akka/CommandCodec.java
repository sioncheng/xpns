package com.github.sioncheng.xpns.server.akka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.util.AssertUtil;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class CommandCodec {

    public CommandCodec() {
        this.decodeStatus = EXPECT_MAGIC;
    }

    public List<JsonCommand> decode(ByteBuffer buffer) {

        if (this.tempBuffer != null) {
            this.tempBuffer = this.tempBuffer.put(buffer);
            buffer = this.tempBuffer;
            this.tempBuffer = null;
        }

        List<JsonCommand> jsonCommands = new ArrayList<>();

        boolean decoding = true;
        int pos = buffer.position();
        int readableBytes = buffer.remaining();
        while(decoding) {
            switch (this.decodeStatus) {
                case EXPECT_MAGIC:
                    if (readableBytes < 2) {
                        decoding = false;
                        break;
                    }

                    AssertUtil.check(Command.MAGIC_BYTE_HIGH == buffer.get(),
                            "check magic byte high");
                    pos += 1;
                    AssertUtil.check(Command.MAGIC_BYTE_LOW == buffer.get(),
                            "check magic byte low");
                    pos += 1;

                    readableBytes -= 2;
                    this.decodeStatus = EXPECT_SERIAL_NUMBER;
                    break;
                case EXPECT_SERIAL_NUMBER:
                    if (readableBytes < 8) {
                        decoding = false;
                        break;
                    }

                    byte[] serialNumberBytes = new byte[8];
                    buffer.get(serialNumberBytes, 0, 8);
                    pos += 8;
                    this.serialNumber = ByteBuffer.wrap(serialNumberBytes).order(ByteOrder.BIG_ENDIAN).getLong();

                    readableBytes -= 8;
                    this.decodeStatus = EXPECT_COMMAND_TYPE;
                    break;
                case EXPECT_COMMAND_TYPE:
                    if (readableBytes < 1) {
                        decoding = false;
                        break;
                    }

                    this.commandType = buffer.get();
                    pos += 1;

                    readableBytes -= 1;
                    this.decodeStatus = EXPECT_SERIALIZATION_TYPE;
                    break;
                case EXPECT_SERIALIZATION_TYPE:
                    if (readableBytes < 1) {
                        decoding = false;
                        break;
                    }

                    this.serializationType = buffer.get();
                    pos += 1;

                    AssertUtil.check(Command.JSON_SERIALIZATION == this.serializationType,
                            "check serialization type");

                    readableBytes -= 1;
                    this.decodeStatus = EXPECT_PAYLOAD_LENGTH;
                    break;
                case EXPECT_PAYLOAD_LENGTH:
                    if (readableBytes < 4) {
                        decoding = false;
                        break;
                    }

                    byte[] payloadLengthBytes = new byte[4];
                    buffer.get(payloadLengthBytes, 0, 4);
                    pos += 4;
                    this.payloadLength = ByteBuffer.wrap(payloadLengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();

                    readableBytes -= 4;
                    this.decodeStatus = EXPECT_PAYLOAD;
                    break;
                case EXPECT_PAYLOAD:
                    if (readableBytes < this.payloadLength) {
                        decoding = false;
                        break;
                    }

                    byte[] payload = new byte[this.payloadLength];
                    buffer.get(payload, 0, this.payloadLength);
                    pos += this.payloadLength;

                    readableBytes -= this.payloadLength;

                    try {
                        JSONObject jsonObject = JSON.parseObject(new String(payload, "UTF-8"));

                        JsonCommand jsonCommand = JsonCommand.create(this.serialNumber,
                                this.commandType,
                                jsonObject);

                        jsonCommands.add(jsonCommand);
                    } catch (UnsupportedEncodingException ue) {
                        decoding = false;
                    }

                    this.decodeStatus = EXPECT_MAGIC;
                    break;
            }
        }

        if (readableBytes > 0) {
            this.tempBuffer = buffer.compact();
        } else {
            this.tempBuffer = null;
        }

        return jsonCommands;
    }


    private int decodeStatus;
    private long serialNumber;
    private byte commandType;
    private byte serializationType;
    private int payloadLength;
    private ByteBuffer tempBuffer;


    private static final int EXPECT_MAGIC = 1;
    private static final int EXPECT_SERIAL_NUMBER = 2;
    private static final int EXPECT_COMMAND_TYPE = 3;
    private static final int EXPECT_SERIALIZATION_TYPE = 4;
    private static final int EXPECT_PAYLOAD_LENGTH = 5;
    private static final int EXPECT_PAYLOAD = 6;
}
