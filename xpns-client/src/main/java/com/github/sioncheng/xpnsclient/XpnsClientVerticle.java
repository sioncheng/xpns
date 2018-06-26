package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.vertx.CommandCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class XpnsClientVerticle extends AbstractVerticle {

    public XpnsClientVerticle(String clientId, NetSocket netSocket) {
        this.commandCodec = new CommandCodec();
        this.status = NEW;
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
        List<JsonCommand> jsonCommands = this.commandCodec.decode(buffer);
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
                    this.status = LOGON;
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
        if (this.status == NEW) {
            logger.warn("not logon {}", this.clientId);
            netSocket.close();
            return;
        }
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

            if (logger.isInfoEnabled()) {
                logger.info(String.format("write command %s %d",
                        jsonCommand.getAcid(), jsonCommand.getCommandCode()));
            }

        } catch (UnsupportedEncodingException ue) {
            logger.warn("write command error", ue);
        }
    }

    private int status;
    private String clientId;
    private NetSocket netSocket;
    private long lastReadWriteTime;
    private CommandCodec commandCodec;

    private static final int NEW = 0;
    private static final int LOGON = 1;

    private static Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
