package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.vertx.CommandCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

public class Client extends AbstractVerticle {

    public Client(ClientServer server, NetSocket netSocket) {
        this.server = server;
        this.netSocket = netSocket;
        this.commandCodec = new CommandCodec();
        this.status = NEW;

        this.netSocket.handler(this::socketHandler);
        this.netSocket.exceptionHandler(this::socketExceptionHandler);
        this.netSocket.closeHandler(this::socketCloseHandler);
    }

    @Override
    public void start() {

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client verticle start %s", this.deploymentID()));
        }

        this.netSocket.resume();

        this.status = START;

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(null);
        event.setEventType(ClientEvent.START);

        this.server.clientEventHandler(event);

        vertx.eventBus().consumer(this.deploymentID(), this::commandHandler);
    }

    @Override
    public void stop()  {

        if (this.netSocket != null) {
            this.netSocket.close();
            this.netSocket = null;
        }

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(null);
        event.setEventType(ClientEvent.STOP);

        this.server.clientEventHandler(event);

        this.publishNotificationEventAddressOnOff(false);

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client verticle stop %s",  this.acid));
        }
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

        this.server.clientEventHandler(event);
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

    private void handleLogin(JsonCommand jsc) {
        this.acid = jsc.getAcid();

        this.status = LOGON;

        JsonCommand jsonCommand = jsc.cloneWithCommandCode(JsonCommand.LOGON_CODE);

        this.writeCommand(jsonCommand, Command.RESPONSE);

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(jsonCommand.getCommandObject());
        event.setEventType(ClientEvent.LOGON);

        this.server.clientEventHandler(event);

        this.publishNotificationEventAddressOnOff(true);
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

        this.server.clientEventHandler(event);
    }

    private void handleHeartbeat(JsonCommand jsonCommand) {
        this.writeCommand(jsonCommand, Command.RESPONSE);

        ClientEvent event = new ClientEvent();
        event.setAcid(this.acid);
        event.setDeploymentId(this.deploymentID());
        event.setCommandObject(jsonCommand.getCommandObject());
        event.setEventType(ClientEvent.HEART_BEAT);

        this.server.clientEventHandler(event);
    }

    private void writeCommand(JsonCommand jsonCommand, byte commandType) {
        try {
            Command command = Command.createJsonCommand(jsonCommand, commandType);

            byte[] data = CommandUtil.serializeCommand(command);

            this.netSocket.write(Buffer.buffer(data));

        } catch (UnsupportedEncodingException ue) {

        }
    }

    private void publishNotificationEventAddressOnOff(boolean on) {
        this.server.publishNotificationEventAddressOnOff(on, this.acid);
    }

    private void commandHandler(Message<String> message) {
        JSONObject jsonObject = JSON.parseObject(message.body());
        JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);
        this.writeCommand(jsonCommand, jsonCommand.getCommandType());
    }

    private ClientServer server;
    private NetSocket netSocket;
    private CommandCodec commandCodec;
    private String acid;
    private int status;

    private static final int NEW = 0;
    private static final int START = 1;
    private static final int LOGON = 2;
    private static final int STOP = 3;

    private static final Logger logger = LoggerFactory.getLogger(Client.class);

}
