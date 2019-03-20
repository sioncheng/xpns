package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSON;
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

    public XpnsClientVerticle(String clientId, NetSocket netSocket, String clientActivationEventAddress) {
        this.commandCodec = new CommandCodec();
        this.status = NEW;
        this.clientId = clientId;
        this.netSocket = netSocket;
        this.lastReadWriteTime = 0;
        this.heatBeatTimerId = 0L;
        this.clientActivationEventAddress = clientActivationEventAddress;
    }

    @Override
    public void start() {
        try {
            super.start();
        } catch (Exception ex) {}

        netSocket.handler(this::socketHandler);
        netSocket.closeHandler(this::socketCloseHandler);
        netSocket.exceptionHandler(this::socketExceptionHandler);

        login();
    }

    @Override
    public void stop() throws Exception {
        if (this.heatBeatTimerId != 0 && !vertx.cancelTimer(this.heatBeatTimerId)) {
            logger.warn(String.format("unable to cancel timer %d of %s", this.heatBeatTimerId, this.clientId));
        }


        if (logger.isInfoEnabled()) {
            logger.info(String.format("xpns client verticle stop %s", clientId));
        }

        super.stop();
    }

    private void socketCloseHandler(Void v) {
        logger.warn("socket closed");
        ClientActivationEvent event = new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT);
        vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(event));
    }

    private void socketExceptionHandler(Throwable t) {
        logger.warn("socket exception", t);
        ClientActivationEvent event = new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT);
        vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(event));
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
                    this.logonTime = System.currentTimeMillis();

                    ClientActivationEvent logonEvent = new ClientActivationEvent(clientId, ClientActivationEvent.LOGON_EVENT);
                    vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(logonEvent));

                    this.heatBeatTimerId = vertx.setPeriodic(200000, this::handlePeriodic);
                    break;
                case JsonCommand.NOTIFICATION_CODE:
                    JSONObject jsonObject = jsonCommand.getCommandObject().getJSONObject(JsonCommand.NOTIFICATION);
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("notification %s", jsonObject.toJSONString()));
                    }

                    ClientActivationEvent notificationEvent = new ClientActivationEvent(clientId, ClientActivationEvent.NOTIFICATION_EVENT);
                    vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(notificationEvent));

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

        writeCommand(jsonCommand, Command.REQUEST);
    }

    private void handlePeriodic(long l) {
        if (this.status == NEW) {
            logger.warn("not logon {}", this.clientId);
            netSocket.close();
            return;
        }
        if (System.currentTimeMillis() - this.logonTime > 20 * 60 * 1000) {
            netSocket.close();
            logger.info("close client to re-connect");
            return;
        }
        if (System.currentTimeMillis() - lastReadWriteTime >= l) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JsonCommand.ACID, this.clientId);
            jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.HEART_BEAT_CODE);
            JsonCommand jsonCommand = JsonCommand.create(Command.HEARTBEAT, jsonObject);

            writeCommand(jsonCommand, Command.HEARTBEAT);
        }
    }

    private void handelNotification(JsonCommand jsonCommand) {
        writeCommand(jsonCommand.cloneWithCommandCode(JsonCommand.ACK_CODE), Command.RESPONSE);
    }

    private void writeCommand(JsonCommand jsonCommand, byte commandType) {
        try {
            Command command = Command.createJsonCommand(jsonCommand, commandType);

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
    private long heatBeatTimerId;
    private String clientActivationEventAddress;
    private long logonTime;

    private static final int NEW = 0;
    private static final int LOGON = 1;


    private static Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
