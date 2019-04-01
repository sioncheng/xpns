package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.vertx.CommandCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class XpnsClientVerticle extends AbstractVerticle {

    public XpnsClientVerticle(NetClient netClient, String remoteHost, int remotePort , String clientId, String clientActivationEventAddress) {
        this.netClient = netClient;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.clientId = clientId;
        this.clientActivationEventAddress = clientActivationEventAddress;

        this.init();
    }

    @Override
    public void start() {
        try {
            super.start();
        } catch (Exception ex) {}

        connect();

    }

    @Override
    public void stop() throws Exception {
        logger.info(String.format("stop %s %d %s", deploymentID(), this.heartBeatTimerId, clientId));

        if (this.heartBeatTimerId != -1 && !vertx.cancelTimer(this.heartBeatTimerId)) {
            logger.warn(String.format("unable to cancel timer %d of %s during stop", this.heartBeatTimerId, this.clientId));
        }


        if (logger.isInfoEnabled()) {
            logger.info(String.format("xpns client verticle stop %s", clientId));
        }

        super.stop();
    }

    private void init() {
        this.commandCodec = new CommandCodec();
        this.status = NEW;
        this.netSocket = null;
        this.lastReadWriteTime = 0;
        this.heartBeatTimerId = -1;
    }

    private void connect() {
        netClient.connect(remotePort, remoteHost, "xpns-server", netSocketAsyncResult -> {
            if (netSocketAsyncResult.succeeded()) {
                logger.info("{} connect to {}:{}", clientId, remoteHost, remotePort);
                netSocket = netSocketAsyncResult.result();
                netSocket.handler(this::socketHandler);
                netSocket.closeHandler(this::socketCloseHandler);
                netSocket.exceptionHandler(this::socketExceptionHandler);


                login();
            } else {
                logger.warn("{} connect to {}:{} failed {}", clientId, remoteHost, remotePort, netSocketAsyncResult.cause());

                vertx.setTimer(3000 + System.currentTimeMillis() % 1000, l -> {
                   vertx.cancelTimer(l);
                   connect();
                });
            }
        });
    }

    private void socketCloseHandler(Void v) {
        logger.warn("{} socket closed", clientId);
        ClientActivationEvent event = new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT);
        vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(event));

        this.netSocket = null;
        this.lastReadWriteTime = System.currentTimeMillis();
        this.status = CLOSE;

        vertx.setTimer(5000 + System.currentTimeMillis() % 1000, l -> {
            logger.info("{} re-connect", clientId);
            vertx.cancelTimer(l);

            if (this.heartBeatTimerId != -1 && !vertx.cancelTimer(this.heartBeatTimerId)) {
                logger.warn(String.format("unable to cancel timer %d of %s during re-connect", this.heartBeatTimerId, this.clientId));
            }

            this.init();

            connect();
        });
    }

    private void socketExceptionHandler(Throwable t) {
        logger.warn("{} socket exception {}", clientId, t);
        ClientActivationEvent event = new ClientActivationEvent(clientId, ClientActivationEvent.CLOSE_EVENT);
        vertx.eventBus().send(clientActivationEventAddress, JSON.toJSONString(event));
    }

    private void socketHandler(Buffer buffer) {
        lastReadWriteTime = System.currentTimeMillis();
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

                    this.heartBeatTimerId = vertx.setPeriodic(HEART_BEAT_INTERVAL / 2, this::handlePeriodic);
                    logger.info(String.format("%s heartBeatTimerId %s", clientId, heartBeatTimerId));
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
                        logger.info("{} heart beat", clientId);
                    }
                    break;
                default:
                    logger.warn(String.format("{} unknown command code %d",clientId, code));
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
        if (System.currentTimeMillis() - this.logonTime > 6 * HEART_BEAT_INTERVAL
                && this.status == LOGON) {
            logger.info("{} close client to re-connect", this.clientId);
            netSocket.close();
            netSocket = null;
            status = CLOSE;
            return;
        }
        if (System.currentTimeMillis() - lastReadWriteTime >= HEART_BEAT_INTERVAL
                && this.status == LOGON) {
            logger.info("{} write heartbeat", this.clientId);

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
                logger.info(String.format("%s write command %s %d",
                        this.clientId, jsonCommand.getAcid(), jsonCommand.getCommandCode()));
            }

        } catch (UnsupportedEncodingException ue) {
            logger.warn("write command error", ue);
        }
    }

    private int status;
    private String remoteHost;
    private int remotePort;
    private String clientId;
    private NetClient netClient;
    private NetSocket netSocket;
    private long lastReadWriteTime;
    private CommandCodec commandCodec;
    private long heartBeatTimerId;
    private String clientActivationEventAddress;
    private long logonTime;

    private static final int NEW = 0;
    private static final int LOGON = 1;
    private static final int CLOSE = 2;

    private static final long HEART_BEAT_INTERVAL = 3 * 60 * 1000;


    private static Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
