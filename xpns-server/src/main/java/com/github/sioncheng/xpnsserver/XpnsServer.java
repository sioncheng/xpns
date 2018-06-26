package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.client.SessionManager;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class XpnsServer implements ClientChannelEventListener {

    public XpnsServer(XpnsServerConfig serverConfig,
                      SessionManager sessionManager,
                      KafkaNotificationManager notificationManager) {
        this.serverConfig = serverConfig;
        this.sessionManager = sessionManager;
        this.notificationManager = notificationManager;
        this.stop = false;
    }

    public boolean start() throws Exception {

        startClientServer();

        startApiServer();

        startWorkerThreads();

        return true;
    }

    public void stop() {
        eventLoopGroupForApi.shutdownGracefully();
        eventLoopGroup.shutdownGracefully();

    }


    @Override
    public void clientChannelActive(ClientChannel clientChannel) {
        if (clientChannelsCounter.incrementAndGet() > this.serverConfig.getMaxClients()) {
            clientChannel.shutdown();
        }
    }

    @Override
    public void commandIn(Command command, ClientChannel clientChannel) {
        int i;
        if (StringUtils.isEmpty(clientChannel.getAcid())) {
            i = random.nextInt(this.clientCommandsList.size());
        } else {
            i = Math.abs(clientChannel.getAcid().hashCode()) % this.clientCommandsList.size();
        }
        this.clientCommandsList.get(i).add(new ClientCommand(command, clientChannel));
    }

    @Override
    public void clientChannelInactive(ClientChannel clientChannel) {
        this.clientChannelsCounter.decrementAndGet();
        String acid = clientChannel.getAcid();
        if (StringUtils.isNotEmpty(acid)) {
            this.sessionManager.removeClient(acid, this.serverConfig.getApiServer());
            Notification notification = this.sendingNotifications.remove(acid);
            if (notification != null) {
                notificationManager.notificationAck(notification, false);
            }
            ConcurrentLinkedQueue<Notification> queue = this.queuingNotifications.remove(acid);
            if (queue != null && queue.size() > 0) {
                for (Notification element :
                        queue) {
                    notificationManager.notificationAck(element, false);
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("client close {}", acid);
        }
    }

    public void notificationIn(Notification notification) {
        ClientChannel clientChannel = this.clientChannels.get(notification.getTo());
        if (clientChannel == null) {
            this.notificationManager.notificationAck(notification, false);
        } else {
            int i = Math.abs(notification.getTo().hashCode()) % this.notificationTasksList.size();
            this.notificationTasksList.get(i).add(notification);
        }
    }

    public SessionInfo getClient(String acid) {
        return this.sessionManager.getClient(acid);
    }

    private void startClientServer() throws InterruptedException {
        this.clientChannels = new ConcurrentHashMap<>();

        this.sendingNotifications = new ConcurrentHashMap<>();
        this.queuingNotifications = new ConcurrentHashMap<>();


        eventLoopGroup = new NioEventLoopGroup(this.serverConfig.getNettyEventLoopGroupThreadsForClient());
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup);
        serverBootstrap.option(ChannelOption.SO_BACKLOG, 256);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(new ClientChannelInitializer(this));

        serverBootstrap.bind(this.serverConfig.getClientPort()).sync();

        if (logger.isInfoEnabled()) {
            logger.info("start client server at {}", this.serverConfig.getClientPort());
        }
    }

    private void startApiServer() throws InterruptedException {
        final XpnsServer xpnsServer = this;

        this.eventLoopGroupForApi = new NioEventLoopGroup(this.serverConfig.getNettyEventLoopGroupThreadsForApi());

        this.serverBootstrapForApi = new ServerBootstrap();
        this.serverBootstrapForApi.group(this.eventLoopGroupForApi);
        this.serverBootstrapForApi.option(ChannelOption.SO_BACKLOG, 64);
        this.serverBootstrapForApi.channel(NioServerSocketChannel.class);
        this.serverBootstrapForApi.childOption(ChannelOption.SO_KEEPALIVE, true);
        this.serverBootstrapForApi.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                ch.pipeline()
                        .addLast("decoder", new HttpRequestDecoder())   // 1
                        .addLast("encoder", new HttpResponseEncoder())  // 2
                        .addLast("aggregator", new HttpObjectAggregator(10 * 1024))    // 3
                        .addLast("handler", new ApiHttpHandler(xpnsServer));        // 4
            }
        });

        InetSocketAddress inetSocketAddress = new InetSocketAddress(this.serverConfig.getApiServer(),
                this.serverConfig.getApiPort());
        this.serverBootstrapForApi.bind(inetSocketAddress).sync();

        if (logger.isInfoEnabled()) {
            logger.info("start api server at {}:{}",
                    this.serverConfig.getApiServer(),
                    this.serverConfig.getApiPort());
        }

    }

    private void startWorkerThreads() {
        this.clientCommandsList = new ArrayList<>(this.serverConfig.getWorkerThreads());
        this.notificationTasksList = new ArrayList<>(this.serverConfig.getWorkerThreads());
        this.serverWorkThreads = new ArrayList<>(this.serverConfig.getWorkerThreads());

        for (int i = 0 ; i < this.serverConfig.getWorkerThreads(); i++) {
            this.clientCommandsList.add(new ConcurrentLinkedQueue<>());
            this.notificationTasksList.add(new ConcurrentLinkedQueue<>());
            final int n = i;
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    workerThreadMethod(n);
                }
            });
            t.setDaemon(true);
            t.setName("xpns-server-worker-thread-" + i);
            this.serverWorkThreads.add(t);
            t.start();
        }
    }

    private void workerThreadMethod(int i) {
        while (!stop) {
            ClientCommand clientCommand = this.clientCommandsList.get(i).poll();
            Notification notification = this.notificationTasksList.get(i).poll();

            if (clientCommand == null && notification == null) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {}

                continue;
            }

            if (clientCommand != null) {
                this.handleClientCommand(clientCommand);
            }

            if (notification != null) {
                this.handleNotification(notification);
            }
        }
    }

    private void handleClientCommand(ClientCommand clientCommand) {

        if (clientCommand.command.getSerializationType() != Command.JSON_SERIALIZATION) {
            clientCommand.clientChannel.shutdown();
            return;
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

            this.sessionManager.putClient(jsonCommand.getAcid(), this.serverConfig.getApiServer());
            this.clientChannels.put(jsonCommand.getAcid(), clientChannel);

            if (logger.isInfoEnabled()) {
                logger.info("client login {}", jsonCommand.getAcid());
            }
        } catch (UnsupportedEncodingException ue) {
            logger.warn("encode command error", ue);

            clientChannel.shutdown();
        }
    }

    private void handleAck(JsonCommand jsonCommand, ClientChannel clientChannel) {
        String acid = jsonCommand.getAcid();
        JSONObject jsonObject = jsonCommand.getCommandObject().getJSONObject(JsonCommand.NOTIFICATION);

        Notification notification = new Notification();
        notification.fromJSONObject(jsonObject);

        this.notificationManager.notificationAck(notification, true);

        this.sendingNotifications.remove(acid);

        ConcurrentLinkedQueue<Notification> queue = this.queuingNotifications.get(acid);
        if (queue != null && queue.size() > 0) {
            int i = Math.abs(acid.hashCode()) % this.notificationTasksList.size();
            this.notificationTasksList.get(i).add(queue.poll());
        }

        if (logger.isInfoEnabled()) {
            logger.info("notification ack {}", notification.toJSONObject().toJSONString());
        }
    }

    private void handleNotification(Notification notification) {

        ClientChannel clientChannel = this.clientChannels.get(notification.getTo());
        if (clientChannel == null) {
            this.notificationManager.notificationAck(notification, false);
            logger.warn("client does not exist {}", notification.getTo());
            return;
        }

        if (sendingNotifications.containsKey(clientChannel.getAcid())) {
            ConcurrentLinkedQueue<Notification> queue;

            if (queuingNotifications.containsKey(clientChannel.getAcid())) {
                queue = queuingNotifications.get(clientChannel.getAcid());
                if (queue.size() > 10) {
                    this.notificationManager.notificationAck(notification, false);

                    if (logger.isInfoEnabled()) {
                        logger.info("client queue is full {}", notification.getTo());
                    }

                    return;
                }
            } else {
                queue = new ConcurrentLinkedQueue<>();
                this.queuingNotifications.put(clientChannel.getAcid(), queue);
            }

            queue.add(notification);

            if (logger.isInfoEnabled()) {
                logger.info("queue notification for client {}", notification.getTo());
            }

            return;

        }

        realSendNotification(notification, clientChannel);

    }

    private void realSendNotification(Notification notification, ClientChannel clientChannel) {
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

            this.sendingNotifications.put(notification.getTo(), notification);

            if (logger.isInfoEnabled()) {
                logger.info("real send notification {}", notification.toJSONObject().toJSONString());
            }

        } catch (UnsupportedEncodingException ue) {
            logger.warn("encode notification error", ue);
        }
    }

    private XpnsServerConfig serverConfig;
    private SessionManager sessionManager;
    private KafkaNotificationManager notificationManager;

    private NioEventLoopGroup eventLoopGroup;
    private ServerBootstrap serverBootstrap;

    private NioEventLoopGroup eventLoopGroupForApi;
    private ServerBootstrap serverBootstrapForApi;


    private ConcurrentHashMap<String, ClientChannel> clientChannels;

    private List<ConcurrentLinkedQueue<ClientCommand>> clientCommandsList;
    private List<ConcurrentLinkedQueue<Notification>> notificationTasksList;
    private List<Thread> serverWorkThreads;


    private ConcurrentHashMap<String, Notification> sendingNotifications;
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<Notification>> queuingNotifications;


    private AtomicInteger clientChannelsCounter = new AtomicInteger(0);
    private volatile boolean stop;

    private static final Logger logger = LoggerFactory.getLogger(XpnsServer.class);
    private static Random random = new Random();


    private class ClientCommand {

        public ClientCommand(Command command, ClientChannel clientChannel) {
            this.command = command;
            this.clientChannel = clientChannel;
        }

        private Command command;
        private ClientChannel clientChannel;
    }
}
