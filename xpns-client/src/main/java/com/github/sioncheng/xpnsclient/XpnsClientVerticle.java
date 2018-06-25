package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.util.AssertUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class XpnsClientVerticle extends AbstractVerticle {

    public XpnsClientVerticle(String clientId, NetSocket netSocket) {
        this.status = 0;
        this.decodeStatus = EXPECT_MAGIC;
        this.clientId = clientId;
        this.netSocket = netSocket;
        this.lastReadWriteTime = 0;
    }

    @Override
    public void start() {
        //super.start();

        netSocket.handler(this::socketHandler);
        netSocket.closeHandler(this::socketCloseHandler);
        netSocket.exceptionHandler(this::socketExceptionHandler);

        login();
    }

    private void socketCloseHandler(Void v) {
        logger.warn("socket closed");
        vertx.eventBus().send(ClientActivationEvent.EVENT_ADDRESS,
                new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT).toJSONObject().toJSONString());
    }

    private void socketExceptionHandler(Throwable t) {
        logger.warn("socket exception", t);
        vertx.eventBus().send(ClientActivationEvent.EVENT_ADDRESS,
                new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT).toJSONObject().toJSONString());
    }

    private void socketHandler(Buffer buffer) {

        if (this.tempBuffer != null) {
            this.tempBuffer = this.tempBuffer.appendBuffer(buffer);
            buffer = this.tempBuffer;
            this.tempBuffer = null;
        }

        List<JsonCommand> jsonCommands = new ArrayList<>();

        boolean decoding = true;
        int pos = 0;
        int readableBytes = buffer.length();
        while(decoding) {
            switch (this.decodeStatus) {
                case EXPECT_MAGIC:
                    if (readableBytes < 2) {
                        decoding = false;
                        break;
                    }

                    AssertUtil.check(Command.MAGIC_BYTE_HIGH == buffer.getByte(pos),
                            "check magic byte high");
                    pos += 1;
                    AssertUtil.check(Command.MAGIC_BYTE_LOW == buffer.getByte(pos),
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
                    buffer.getBytes(pos, pos + 8, serialNumberBytes);
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

                    this.commandType = buffer.getByte(pos);
                    pos += 1;

                    readableBytes -= 1;
                    this.decodeStatus = EXPECT_SERIALIZATION_TYPE;
                    break;
                case EXPECT_SERIALIZATION_TYPE:
                    if (readableBytes < 1) {
                        decoding = false;
                        break;
                    }

                    this.serializationType = buffer.getByte(pos);
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
                    buffer.getBytes(pos, pos+4, payloadLengthBytes);
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
                    buffer.getBytes(pos, pos + this.payloadLength, payload);
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
            this.tempBuffer = buffer.getBuffer(pos, pos + readableBytes);
        } else {
            this.tempBuffer = null;
        }

        if (jsonCommands.size() > 0) {
            processCommands(jsonCommands);
        }
    }

    private void processCommands(List<JsonCommand> jsonCommands) {
        for (JsonCommand jsonCommand :
                jsonCommands) {

            int code = jsonCommand.getCommandCode();
            switch (code) {
                case JsonCommand.LOGON_CODE:
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("logon %s", clientId));
                    }
                    vertx.eventBus().send(ClientActivationEvent.EVENT_ADDRESS,
                            new ClientActivationEvent(clientId, ClientActivationEvent.LOGON_EVENT).toJSONObject().toJSONString());

                    vertx.setPeriodic(300000, this::handlePeriodic);
                    break;
                case JsonCommand.NOTIFICATION_CODE:
                    JSONObject jsonObject = jsonCommand.getCommandObject().getJSONObject(JsonCommand.NOTIFICATION);
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("notification %s", jsonObject.toJSONString()));
                    }
                    vertx.eventBus().send(ClientActivationEvent.EVENT_ADDRESS,
                            new ClientActivationEvent(clientId, ClientActivationEvent.NOTIFICATION_EVENT).toJSONObject().toJSONString());
                    this.handelNotification(jsonCommand);
                    break;
                case JsonCommand.HEART_BEAT_CODE:
                    if (logger.isInfoEnabled()) {
                        logger.info("heart beat");
                    }
                    break;
                default:
                    logger.warn(String.format("unknown command code %d", code));
                    break;
            }
        }
    }

    private void login() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JsonCommand.ACID, clientId);
        jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.LOGIN_CODE);

        JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);

        writeCommand(jsonCommand);
    }

    private void handlePeriodic(long l) {
        if (System.currentTimeMillis() - lastReadWriteTime >= l) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JsonCommand.ACID, this.clientId);
            JsonCommand jsonCommand = JsonCommand.create(Command.HEARTBEAT, jsonObject);

            writeCommand(jsonCommand);
        }
    }

    private void handelNotification(JsonCommand jsonCommand) {
        jsonCommand.setCommandCode(JsonCommand.ACK_CODE);

        writeCommand(jsonCommand);
    }

    private void writeCommand(JsonCommand jsonCommand) {
        try {
            Command command = new Command();
            command.setSerialNumber(jsonCommand.getSerialNumber());
            command.setCommandType(jsonCommand.getCommandType());
            command.setSerializationType(Command.JSON_SERIALIZATION);
            byte[] payload = jsonCommand.getCommandObject().toJSONString().getBytes("UTF-8");
            command.setPayloadLength(payload.length);
            command.setPayloadBytes(payload);

            netSocket.write(Buffer.buffer(CommandUtil.serializeCommand(command)));

            lastReadWriteTime = System.currentTimeMillis();
        } catch (UnsupportedEncodingException ue) {
            logger.warn("write command error", ue);
        }
    }

    private int status;
    private int decodeStatus;
    private String clientId;
    private NetSocket netSocket;

    private long lastReadWriteTime;


    private long serialNumber;
    private byte commandType;
    private byte serializationType;
    private int payloadLength;
    private Buffer tempBuffer;


    private static final int EXPECT_MAGIC = 1;
    private static final int EXPECT_SERIAL_NUMBER = 2;
    private static final int EXPECT_COMMAND_TYPE = 3;
    private static final int EXPECT_SERIALIZATION_TYPE = 4;
    private static final int EXPECT_PAYLOAD_LENGTH = 5;
    private static final int EXPECT_PAYLOAD = 6;

    private static Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
