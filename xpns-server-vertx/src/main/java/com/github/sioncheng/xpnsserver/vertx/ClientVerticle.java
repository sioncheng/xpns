package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.vertx.CommandCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ClientVerticle extends AbstractVerticle {

    public ClientVerticle(int serverId, String clientEventAddress, NetSocket netSocket) {
        this.serverId = serverId;
        this.clientEventAddress = clientEventAddress;
        this.netSocket = netSocket;
        this.commandCodec = new CommandCodec();
        this.status = NEW;
    }

    @Override
    public void start() throws Exception {

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client verticle start %s", this.deploymentID()));
        }

        this.netSocket.handler(this::socketHandler);
        this.netSocket.exceptionHandler(this::socketExceptionHandler);
        this.netSocket.closeHandler(this::socketCloseHandler);

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(null);
        event.setEventType(ClientEvent.START);

        vertx.eventBus().send(clientEventAddress, JSON.toJSONString(event));

        super.start();
    }

    @Override
    public void stop() throws Exception {

        if (this.netSocket != null) {
            this.netSocket.close();
            this.netSocket = null;
        }

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(null);
        event.setEventType(ClientEvent.STOP);

        vertx.eventBus().send(clientEventAddress, JSON.toJSONString(event));

        this.publishNotificationEventAddressOnOff(false);

        if (this.notificationConsumer != null) {
            this.notificationConsumer.unregister();
        }

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client verticle stop %s",  this.acid));
        }

        super.stop();
    }

    private void socketHandler(Buffer buffer) {
        List<JsonCommand> jsonCommands = this.commandCodec.decode(buffer);
        if (jsonCommands.size() > 0) {
            for (JsonCommand jc :
                    jsonCommands) {
                this.handleCommand(jc);
            }
        }
    }

    private void socketExceptionHandler(Throwable t) {
        logger.warn("socket exception handler", t);

        close();
    }

    private void socketCloseHandler(Void v) {

        if (logger.isInfoEnabled()) {
            logger.info(String.format("socket closed %s", this.acid));
        }

        close();
    }

    private void close() {
        this.status = STOP;

        if (this.netSocket == null) {
            if (logger.isInfoEnabled()) {
                logger.info("un deploy and stop");
                return;
            }
        }

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(null);
        event.setEventType(ClientEvent.SOCKET_CLOSE);

        vertx.eventBus().send(clientEventAddress, JSON.toJSONString(event));
    }

    private void handleCommand(JsonCommand jsonCommand) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle command %s", jsonCommand.getCommandObject().toJSONString()));
        }

        int commandCode = jsonCommand.getCommandCode();
        switch (commandCode) {
            case JsonCommand.LOGIN_CODE:
                this.handleLogin(jsonCommand);
                break;
            case JsonCommand.ACK_CODE:
                this.handleAck(jsonCommand);
                break;
            case JsonCommand.HEART_BEAT_CODE:
                this.handleHeartbeat(jsonCommand);
                break;
        }
    }

    private void handleLogin(JsonCommand jsonCommand) {
        this.acid = jsonCommand.getAcid();

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(jsonCommand.getCommandObject());
        event.setEventType(ClientEvent.LOGON);

        vertx.eventBus().send(clientEventAddress, JSON.toJSONString(event));


        this.notificationEventAddress = NotificationEvent.EVENT_ADDRESS_PREFIX + "." + this.deploymentID();

        this.publishNotificationEventAddressOnOff(true);

        vertx.eventBus().consumer(this.notificationEventAddress, this::notificationEventHandler);

        this.status = LOGON;
        jsonCommand.setCommandCode(JsonCommand.LOGON_CODE);

        this.writeCommand(jsonCommand, Command.RESPONSE);
    }

    private void handleAck(JsonCommand jsonCommand) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("notification ack %s", jsonCommand.getCommandObject().toJSONString()));
        }

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(jsonCommand.getCommandObject());
        event.setEventType(ClientEvent.ACK);

        vertx.eventBus().send(clientEventAddress, JSON.toJSONString(event));
    }

    private void handleHeartbeat(JsonCommand jsonCommand) {
        this.writeCommand(jsonCommand, Command.RESPONSE);
    }

    private void writeCommand(JsonCommand jsonCommand, byte commandType) {
        try {
            Command command = new Command();

            command.setSerialNumber(jsonCommand.getSerialNumber());
            command.setCommandType(commandType);
            command.setSerializationType(Command.JSON_SERIALIZATION);

            byte[] payload = jsonCommand.getCommandObject().toJSONString().getBytes("UTF-8");
            command.setPayloadLength(payload.length);
            command.setPayloadBytes(payload);

            byte[] data = CommandUtil.serializeCommand(command);

            this.netSocket.write(Buffer.buffer(data));

        } catch (UnsupportedEncodingException ue) {

        }
    }

    private void publishNotificationEventAddressOnOff(boolean on) {
        NotificationEventAddressBroadcast neab = new NotificationEventAddressBroadcast();
        neab.setOn(on);
        neab.setAcid(this.acid);
        neab.setEventAddress(this.notificationEventAddress);

        vertx.eventBus().publish(NotificationEventAddressBroadcast.EVENT_ADDRESS,
                JSON.toJSONString(neab));


    }

    private void notificationEventHandler(Message<String> msg) {
        Notification notification = JSON.parseObject(msg.body(), Notification.class);

        if (this.status == STOP) {

            return;
            //
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JsonCommand.ACID, notification.getTo());
        jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.NOTIFICATION_CODE);
        jsonObject.put(JsonCommand.NOTIFICATION, notification.toJSONObject());

        JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);

        this.writeCommand(jsonCommand, Command.REQUEST);
    }

    private int serverId;
    private String clientEventAddress;
    private String notificationEventAddress;
    private NetSocket netSocket;
    private CommandCodec commandCodec;
    private String acid;

    private MessageConsumer<String> notificationConsumer;

    private int status;

    private static final int NEW = 0;
    private static final int LOGON = 1;
    private static final int STOP = 2;

    private static final Logger logger = LoggerFactory.getLogger(ClientVerticle.class);

}
