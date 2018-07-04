package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.zk.Directories;
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
import org.apache.zookeeper.*;
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
                      KafkaNotificationAckManager notificationManager,
                      String zkServers) {
        this.serverConfig = serverConfig;
        this.sessionManager = sessionManager;
        this.notificationManager = notificationManager;
        this.zkServers = zkServers;
        this.stop = false;
    }

    public boolean start() throws Exception {

        startClientServer();

        startApiServer();

        startWorkerThreads();

        registerToZk();

        return true;
    }

    public void stop() {
        eventLoopGroupMasterForApi.shutdownGracefully();
        eventLoopGroupForApi.shutdownGracefully();
        eventLoopGroupMaster.shutdownGracefully();
        eventLoopGroup.shutdownGracefully();

    }


    @Override
    public void clientChannelActive(ClientChannel clientChannel) {
        int clients = clientChannelsCounter.get();

        if (clients >= this.serverConfig.getMaxClients()) {
            logger.warn("reach max clients {} {}", clients, this.serverConfig.getMaxClients());
            clientChannel.shutdown();
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("client channel active {}", clients);
            }
            clientChannelsCounter.incrementAndGet();
        }
    }

    @Override
    public void commandIn(Command command, ClientChannel clientChannel) {
        int i;
        if (StringUtils.isEmpty(clientChannel.getAcid())) {
            i = random.nextInt(this.clientCommandsList.size());
            if (logger.isInfoEnabled()) {
                logger.info("random dispatch command {} to {}", command.getSerialNumber(), i);
            }
        } else {
            i = Math.abs(clientChannel.getAcid().hashCode()) % this.clientCommandsList.size();
            if (logger.isInfoEnabled()) {
                logger.info("dispatch command {} to {}", command.getSerialNumber(), i);
            }
        }
        boolean b = this.clientCommandsList.get(i).add(new ClientCommand(command, clientChannel));
        if (logger.isInfoEnabled()) {
            logger.info("add command {} to {} result {}",
                    command.getSerialNumber(), i, b);
        }
    }

    @Override
    public void clientChannelInactive(ClientChannel clientChannel) {
        this.clientChannelsCounter.decrementAndGet();
        String acid = clientChannel.getAcid();
        if (StringUtils.isNotEmpty(acid)) {
            int i = Math.abs(acid.hashCode()) % this.clientInactiveEventsList.size();
            this.clientInactiveEventsList.get(i).add(clientChannel);
        }

        if (logger.isInfoEnabled()) {
            logger.info("client close {}", acid);
        }
    }

    public void notificationIn(Notification notification) {
        ClientChannel clientChannel = this.clientChannels.get(notification.getTo());
        if (clientChannel == null) {
            if (logger.isWarnEnabled()) {
                logger.warn("client does not exists {}", notification.getTo());
            }
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
        eventLoopGroupMaster = new NioEventLoopGroup(1);
        eventLoopGroup = new NioEventLoopGroup(this.serverConfig.getNettyEventLoopGroupThreadsForClient());
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroupMaster,eventLoopGroup);
        serverBootstrap.option(ChannelOption.SO_REUSEADDR, true);
        serverBootstrap.option(ChannelOption.SO_TIMEOUT, 360000);
        serverBootstrap.channel(NioServerSocketChannel.class);
        serverBootstrap.childHandler(new ClientChannelInitializer(this));

        serverBootstrap.bind(this.serverConfig.getClientPort()).sync();

        if (logger.isInfoEnabled()) {
            logger.info("start client server at {}", this.serverConfig.getClientPort());
        }
    }

    private void startApiServer() throws InterruptedException {
        final XpnsServer xpnsServer = this;

        this.eventLoopGroupMasterForApi = new NioEventLoopGroup(1);
        this.eventLoopGroupForApi = new NioEventLoopGroup(this.serverConfig.getNettyEventLoopGroupThreadsForApi());

        this.serverBootstrapForApi = new ServerBootstrap();
        this.serverBootstrapForApi.group(this.eventLoopGroupMasterForApi, this.eventLoopGroupForApi);
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
        this.serverBootstrapForApi.bind(this.serverConfig.getApiPort()).sync();

        if (logger.isInfoEnabled()) {
            logger.info("start api server at {}:{}",
                    this.serverConfig.getApiServer(),
                    this.serverConfig.getApiPort());
        }

    }

    private void startWorkerThreads() {
        this.clientChannels = new ConcurrentHashMap<>();
        this.sendingNotifications = new ConcurrentHashMap<>();
        this.queuingNotifications = new ConcurrentHashMap<>();

        this.clientInactiveEventsList = new ArrayList<>(this.serverConfig.getWorkerThreads());
        this.clientCommandsList = new ArrayList<>(this.serverConfig.getWorkerThreads());
        this.notificationTasksList = new ArrayList<>(this.serverConfig.getWorkerThreads());
        this.serverWorkThreads = new ArrayList<>(this.serverConfig.getWorkerThreads());

        for (int i = 0 ; i < this.serverConfig.getWorkerThreads(); i++) {
            this.clientInactiveEventsList.add(new ConcurrentLinkedQueue<>());
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

    private void registerToZk() throws Exception {
        this.zooKeeper = new ZooKeeper(this.zkServers, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });

        String root = Directories.XPNS_SERVER_ROOT;
        if (this.zooKeeper.exists(root, false) == null) {
            this.zooKeeper.create(root, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String apiServices = root + "/" + Directories.API_SERVICES;
        if (this.zooKeeper.exists(apiServices, false) == null) {
            this.zooKeeper.create(apiServices, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String servicePath = apiServices + "/" +
                this.serverConfig.getApiServer() + ":" + this.serverConfig.getApiPort();
        this.zooKeeper.create(servicePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    private void workerThreadMethod(int i) {
        int continueCount = 0;
        while (!stop) {
            ClientChannel clientChannel = this.clientInactiveEventsList.get(i).poll();
            ClientCommand clientCommand = this.clientCommandsList.get(i).poll();
            Notification notification = this.notificationTasksList.get(i).poll();

            if (clientChannel == null && clientCommand == null && notification == null) {
                try {
                    Thread.sleep(10);
                } catch (Exception ex) {}

                continueCount++;

                if (continueCount >= 6000) {
                    if (logger.isInfoEnabled()) {
                        logger.info("there is no work to do {} {}", i, Thread.currentThread().getName());
                    }
                    continueCount = 0;
                }

                continue;
            }
            continueCount = 0;

            if (clientChannel != null) {
                this.handleChannelInactive(clientChannel);
            }

            if (clientCommand != null) {
                this.handleClientCommand(clientCommand);
            }

            if (notification != null) {
                this.handleNotification(notification);
            }
        }
        logger.info("exit worker thread method {} {}", i, Thread.currentThread().getName());
    }

    private void handleChannelInactive(ClientChannel clientChannel) {
        String acid = clientChannel.getAcid();
        this.clientChannels.remove(acid);
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

        ClientChannel existedClientChannel = this.clientChannels.get(jsonCommand.getAcid());
        if (existedClientChannel != null) {
            logger.warn("existed client channel {}", jsonCommand.getAcid());
            existedClientChannel.shutdown(false);
            this.handleChannelInactive(existedClientChannel);
        }

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

            SessionInfo sessionInfo = new SessionInfo();
            sessionInfo.setAcid(jsonCommand.getAcid());
            sessionInfo.setServer(this.serverConfig.getApiServer());
            sessionInfo.setPort(this.serverConfig.getApiPort());
            this.sessionManager.putClient(sessionInfo);
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
            logger.warn("client does not exist {}", notification.getTo());
            this.notificationManager.notificationAck(notification, false);
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
    private KafkaNotificationAckManager notificationManager;
    private String zkServers;
    private ZooKeeper zooKeeper;

    private NioEventLoopGroup eventLoopGroupMaster;
    private NioEventLoopGroup eventLoopGroup;
    private ServerBootstrap serverBootstrap;

    private NioEventLoopGroup eventLoopGroupMasterForApi;
    private NioEventLoopGroup eventLoopGroupForApi;
    private ServerBootstrap serverBootstrapForApi;


    private ConcurrentHashMap<String, ClientChannel> clientChannels;

    private List<ConcurrentLinkedQueue<ClientChannel>> clientInactiveEventsList;
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
