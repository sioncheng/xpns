package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionManager;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class XpnsServer implements ClientChannelEventListener {

    public XpnsServer(int maxClients, SessionManager sessionManager, String exposeHost) {
        this.maxClients = maxClients;
        this.sessionManager = sessionManager;
        this.stop = false;
        this.exposeServer = exposeHost;
    }

    public boolean start(int port
            , int nettyEventLoopGroupThreads) throws Exception {


        this.clientChannels = new ConcurrentHashMap<>();
        this.clientCommands = new ConcurrentLinkedQueue<>();
        this.handleClientCommandsThread = new Thread(new Runnable() {
            @Override
            public void run() {
                handleClientCommand();
            }
        });
        this.handleClientCommandsThread.start();

        this.notifications = new ConcurrentLinkedQueue<>();
        this.handleNotificationThread = new Thread(new Runnable() {
            @Override
            public void run() {
                handleNotification();
            }
        });
        this.handleNotificationThread.start();

        eventLoopGroup = new NioEventLoopGroup(nettyEventLoopGroupThreads);
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.option(ChannelOption.SO_BACKLOG, 256);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(new ClientChannelInitializer(this));

        serverBootstrap.bind(port).sync();

        return true;
    }

    public void stop() {

        eventLoopGroup.shutdownGracefully();

    }

    public boolean sendNotification(Notification notification) {
        ClientChannel clientChannel = this.clientChannels.get(notification.getTo());
        if (clientChannel == null) {
            return false;
        }

        return this.notifications.add(notification);
    }

    @Override
    public void clientChannelActive(ClientChannel clientChannel) {
        if (clientChannelsCounter.incrementAndGet() > this.maxClients) {
            clientChannel.shutdown();
        }
    }

    @Override
    public void commandIn(Command command, ClientChannel clientChannel) {
        this.clientCommands.add(new ClientCommand(command, clientChannel));
    }

    @Override
    public void clientChannelInactive(ClientChannel clientChannel) {
        clientChannelsCounter.decrementAndGet();

        this.sessionManager.removeClient(clientChannel.getAcid(), this.exposeServer);
    }

    private void handleClientCommand() {
        while (!stop) {
            ClientCommand clientCommand = this.clientCommands.poll();
            if (clientCommand == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) { }

                continue;
            }

            if (clientCommand.command.getSerializationType() != Command.JSON_SERIALIZATION) {
                clientCommand.clientChannel.shutdown();
                continue;
            }

            switch (clientCommand.command.getCommandType()) {
                case Command.HEARTBEAT:
                    this.handleHeartbeat(clientCommand);
                    break;
                case Command.REQUEST:
                    this.handleRequest(clientCommand);
                    break;
                case Command.RESPONSE:
                    this.handleResponse(clientCommand);
                    break;
                default:
                    clientCommand.clientChannel.shutdown();
                    break;
            }
        }
    }

    private void handleHeartbeat(ClientCommand clientCommand) {
        clientCommand.command.setCommandType(Command.HEARTBEAT);
        clientCommand.clientChannel.writeCommand(clientCommand.command);
    }

    private void handleRequest(ClientCommand clientCommand) {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(new String(clientCommand.command.getPayloadBytes(), "UTF-8"));
        } catch (UnsupportedEncodingException ue) {
            logger.warn("decode command error", ue);
        }

        if (jsonObject == null) {
            clientCommand.clientChannel.shutdown();
            return;
        }

        JsonCommand jsonCommand = JsonCommand.create(clientCommand.command.getSerialNumber(),
                clientCommand.command.getCommandType(),
                jsonObject);

        this.handleCommand(jsonCommand, clientCommand.clientChannel);
    }


    private void handleResponse(ClientCommand clientCommand) {
        JSONObject jsonObject = null;
        try {
            jsonObject = JSON.parseObject(new String(clientCommand.command.getPayloadBytes(), "UTF-8"));
        } catch (UnsupportedEncodingException ue) {
            logger.warn("decode command error", ue);
        }

        if (jsonObject == null) {
            clientCommand.clientChannel.shutdown();
            return;
        }

        JsonCommand jsonCommand = JsonCommand.create(clientCommand.command.getSerialNumber(),
                clientCommand.command.getCommandType(),
                jsonObject);

        this.handleCommand(jsonCommand, clientCommand.clientChannel);
    }


    private void handleCommand(JsonCommand jsonCommand, ClientChannel clientChannel) {
        switch (jsonCommand.getCommandCode()) {
            case JsonCommand.LOGIN_CODE:
                this.handleLogin(jsonCommand, clientChannel);
                break;
            case JsonCommand.ACK_CODE:
                this.handleAck(jsonCommand, clientChannel);
                break;
            default:
                clientChannel.shutdown();
                break;
        }
    }

    private void handleLogin(JsonCommand jsonCommand, ClientChannel clientChannel) {

        try {

            clientChannel.setAcid(jsonCommand.getAcid());

            JSONObject jsonObject = jsonCommand.getCommandObject();
            jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.LOGON_CODE);

            JsonCommand jsonCommand1 = JsonCommand.create(jsonCommand.getSerialNumber(), Command.RESPONSE, jsonObject);
            byte[] payload = jsonCommand1.getCommandObject().toJSONString().getBytes("UTF-8");

            Command command = new Command(jsonCommand1.getSerialNumber(),
                    Command.RESPONSE,
                    Command.JSON_SERIALIZATION,
                    payload.length,
                    payload);
            clientChannel.writeCommand(command);

            this.sessionManager.putClient(jsonCommand.getAcid(), this.exposeServer);
            this.clientChannels.put(jsonCommand.getAcid(), clientChannel);
        } catch (UnsupportedEncodingException ue) {
            logger.warn("encode command error", ue);

            clientChannel.shutdown();
        }
    }

    private void handleAck(JsonCommand jsonCommand, ClientChannel clientChannel) {

    }

    private void handleNotification() {
        while (!stop) {
            Notification notification = this.notifications.poll();

            if (notification == null) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex){}

                continue;
            }

            ClientChannel clientChannel = this.clientChannels.get(notification.getTo());
            if (clientChannel == null) {
                //
                //todo, notification ack failure

                continue;
            }

            try {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.NOTIFICATION_CODE);
                jsonObject.put(JsonCommand.ACID, notification.getTo());
                jsonObject.put(JsonCommand.NOTIFICATION, notification.toJSONObject());


                JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);

                Command command = new Command();
                command.setSerialNumber(jsonCommand.getSerialNumber());
                command.setCommandType(Command.REQUEST);
                command.setSerializationType(Command.JSON_SERIALIZATION);

                byte[] payload = jsonCommand.getCommandObject().toJSONString().getBytes("UTF-8");
                command.setPayloadLength(payload.length);
                command.setPayloadBytes(payload);

                clientChannel.writeCommand(command);


            } catch (UnsupportedEncodingException ue) {
                logger.warn("encode notification error", ue);
            }
        }
    }

    private NioEventLoopGroup eventLoopGroup;
    private ServerBootstrap serverBootstrap;
    private SessionManager sessionManager;

    private ConcurrentHashMap<String, ClientChannel> clientChannels;

    private ConcurrentLinkedQueue<ClientCommand> clientCommands;
    private Thread handleClientCommandsThread;

    private ConcurrentLinkedQueue<Notification> notifications;
    private Thread handleNotificationThread;

    private String exposeServer;

    private int maxClients;
    private AtomicInteger clientChannelsCounter = new AtomicInteger(0);
    private volatile boolean stop;

    private static final Logger logger = LoggerFactory.getLogger(XpnsServer.class);


    private class ClientCommand {

        public ClientCommand(Command command, ClientChannel clientChannel) {
            this.command = command;
            this.clientChannel = clientChannel;
        }

        private Command command;
        private ClientChannel clientChannel;
    }
}
