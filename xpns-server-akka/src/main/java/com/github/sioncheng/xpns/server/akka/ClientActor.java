package com.github.sioncheng.xpns.server.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.CommandUtil;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

public class ClientActor extends AbstractActor {

    public static Props props(ActorRef server, ActorRef remote) {
        return Props.create(ClientActor.class,() -> new ClientActor(server, remote));
    }

    public ClientActor(ActorRef server, ActorRef remote) {
        this.server = server;
        this.remote = remote;
        this.version = UUID.randomUUID().toString();
        this.logger = Logging.getLogger(getContext().getSystem(), this);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, this::handleReceived)
                .match(Tcp.ConnectionClosed.class, connectionClosed -> {
                    server.tell(ClientActivation.createLogout(getSelf(), acid, version), getSelf());
                    getContext().stop(self());
                })
                .match(Notification.class, this::handleNotificationRequest)
                .build();
    }

    private void handleReceived(Tcp.Received received) {
        List<JsonCommand> commands = null;
        try {
            commands = commandCodec.decode(received.data().asByteBuffer());
        } catch (Error ex) {
            logger.warning("decode error", ex);
            getContext().stop(getSelf());
            return;
        }

        if (commands != null) {
            commands.forEach(c -> {
                switch (c.getCommandCode()) {
                    case JsonCommand.LOGIN_CODE:
                        handleLogin(c);
                        break;
                    case JsonCommand.HEART_BEAT_CODE:
                        handleHeartbeat(c);
                        break;
                    case JsonCommand.ACK_CODE:
                        handleAck(c);
                        break;
                }
            });
        }
    }

    private void handleLogin(JsonCommand c) {

        try {
            JSONObject responseObject = (JSONObject) c.getCommandObject().clone();
            responseObject.put(JsonCommand.COMMAND_CODE, JsonCommand.LOGON_CODE);
            JsonCommand response = JsonCommand.create(c.getSerialNumber(), Command.RESPONSE, responseObject);
            Command command = Command.createJsonCommand(response, Command.RESPONSE);
            byte[] data = CommandUtil.serializeCommand(command);

            getSender().tell(TcpMessage.write(ByteString.fromArray(data)), getSelf());

            this.acid = c.getAcid();
            server.tell(ClientActivation.createLogon(getSelf(), c.getAcid(), version), getSelf());
        } catch (UnsupportedEncodingException ue) {
            logger.warning("handle login error", ue);

            getContext().stop(getSelf());
        }
    }

    private void handleHeartbeat(JsonCommand c) {

        try {
            JSONObject responseObject = (JSONObject) c.getCommandObject().clone();
            responseObject.put(JsonCommand.COMMAND_CODE, JsonCommand.HEART_BEAT_CODE);
            JsonCommand response = JsonCommand.create(c.getSerialNumber(), Command.RESPONSE, responseObject);
            Command command = Command.createJsonCommand(response, Command.RESPONSE);
            byte[] data = CommandUtil.serializeCommand(command);

            getSender().tell(TcpMessage.write(ByteString.fromArray(data)), getSelf());

            server.tell(ClientActivation.createHearbeat(getSelf(), c.getAcid(), version), getSelf());

        } catch (UnsupportedEncodingException ue) {
            logger.warning("handle login error", ue);

            getContext().stop(getSelf());
        }
    }

    private void handleAck(JsonCommand c) {
        if (logger.isInfoEnabled()){
            logger.info(String.format("notification ack %s", c.getCommandObject().toString()));
        }

        ClientActivation clientActivation = ClientActivation.createAck(getSelf(),
                c.getAcid(),
                version,
                c.getCommandObject());

        server.tell(clientActivation, getSelf());
    }

    private void handleNotificationRequest(Notification notificationRequest) {
        try {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put(JsonCommand.ACID, notificationRequest.getTo());
            jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.NOTIFICATION_CODE);
            jsonObject.put(JsonCommand.NOTIFICATION, notificationRequest.toJSONObject());

            JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);
            Command command = Command.createJsonCommand(jsonCommand, Command.REQUEST);
            byte[] data = CommandUtil.serializeCommand(command);

            remote.tell(TcpMessage.write(ByteString.fromArray(data)), getSelf());

        } catch (UnsupportedEncodingException ue) {
            logger.warning("handle notification request error", ue);
        }
    }

    private ActorRef server;

    private ActorRef remote;

    private String acid;

    private String version;

    private LoggingAdapter logger;

    private CommandCodec commandCodec = new CommandCodec();
}
