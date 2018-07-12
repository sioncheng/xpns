package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.protocol.Command;
import com.github.sioncheng.xpns.common.protocol.JsonCommand;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ClientServer extends AbstractVerticle {

    public ClientServer(int id, int port, int maxClients, int instances,
                        RedisOptions redisOptions, String apiHost, int apiPort,
                        Map<String, String> kafakProducerConfig,
                        String notificationAckTopic,
                        String logonTopic) {
        this.id = id;
        this.port = port;
        this.maxClients = maxClients;
        this.instances = instances;
        this.redisOptions = redisOptions;
        this.apiHost = apiHost;
        this.apiPort = apiPort;
        this.kafkaProducerConfig = kafakProducerConfig;
        this.notificationAckTopic = notificationAckTopic;
        this.logonTopic = logonTopic;
        this.clientsCounter = 0;
        this.logonCounter = 0;
        this.clientTableByDepId = new HashMap<>();
        this.clientTableByAcid = new HashMap<>();
        this.clientEventAddress = ClientEvent.EVENT_ADDRESS_PREFIX + "." + this.id;
        this.notificationEventAddress = NotificationEvent.EVENT_ADDRESS_PREFIX + "." + this.id;
    }

    @Override
    public void start() throws Exception {

        this.redisClient = RedisClient.create(vertx, this.redisOptions);

        NetServer netServer = vertx.createNetServer();
        netServer.connectHandler(this::connectHandler);
        netServer.listen(port,result->{
            if (logger.isInfoEnabled()) {
                logger.info(String.format("bind result %s", Boolean.toString(result.succeeded())));
            }
        });

        vertx.eventBus().consumer(this.clientEventAddress, this::clientEventMessageHandler);
        vertx.eventBus().consumer(NotificationAskEvent.EVENT_ADDRESS, this::notificationAskEventHandler);
        vertx.eventBus().consumer(this.notificationEventAddress, this::notificationEventHandler);

        kafkaProducer = KafkaProducer.create(vertx, this.kafkaProducerConfig);

        super.start();
    }

    @Override
    public void stop() throws Exception {

        super.stop();
    }

    private void connectHandler(NetSocket netSocket) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("connect handler %s", netSocket.remoteAddress().toString()));
        }

        if (this.clientsCounter >= this.maxClients) {
            logger.warn(String.format("reach the max clients %d", this.maxClients));
            netSocket.close();

            return;
        }

        this.clientsCounter++;
        if (logger.isInfoEnabled()) {
            logger.info(String.format("connected clients %d", this.clientsCounter));
        }

        Client client = new Client(this, netSocket);

        ClientVerticleEntity cce = new ClientVerticleEntity();
        cce.setAcid("");
        cce.setDeploymentId(client.deploymentID());
        cce.setClient(client);
        this.clientTableByDepId.put(cce.getDeploymentId(), cce);

        client.start();
    }

    private void clientEventMessageHandler(Message<String> msg) {
        ClientEvent event = JSON.parseObject(msg.body(), ClientEvent.class);

        this.clientEventHandler(event);
    }


    public void clientEventHandler(ClientEvent event) {

        switch (event.getEventType()) {
            case ClientEvent.START:
                if (logger.isInfoEnabled()) {

                }
                break;
            case ClientEvent.LOGON:
                this.handleLogon(event);
                break;
            case ClientEvent.SOCKET_CLOSE:
                this.handleSocketClose(event);
                break;
            case ClientEvent.STOP:
                this.handleClientVerticleStop(event);
                break;
            case ClientEvent.LOGON_FOWARD:
                this.handleLogonForward(event);
                break;
            case ClientEvent.ACK:
                this.handleNotificationAck(event);
                break;
            default:
                logger.warn(String.format("unknown event %s", event.getEventType()));
                break;

        }
    }

    private void handleLogon(ClientEvent event) {
        this.logonCounter++;
        if (logger.isInfoEnabled()) {
            logger.info(String.format("client logon %s %d", event.getAcid(), this.logonCounter));
        }

        //set to redis
        SessionInfo sessionInfo = new SessionInfo();
        sessionInfo.setAcid(event.getAcid());
        sessionInfo.setServer(this.apiHost);
        sessionInfo.setPort(this.apiPort);

        String sessionInfoJson = JSON.toJSONString(sessionInfo);

        final String onlineKey = RedisHelper.generateOnlineKey(sessionInfo.getAcid());
        this.redisClient.set(onlineKey,
                sessionInfoJson,
                result ->{
                    if (result.succeeded()) {
                        this.redisClient.expire(onlineKey, 3600, null);
                    } else {
                        logger.warn(String.format("set online info to redis failure %s", onlineKey));
                    }
                });

        ClientVerticleEntity cce = this.clientTableByDepId.get(event.getDeploymentId());
        ClientVerticleEntity cce2 = this.clientTableByAcid.get(event.getAcid());

        //if there is the same client, close it.
        if (cce2 != null) {
            vertx.undeploy(cce2.getDeploymentId());
            this.clientTableByDepId.remove(cce2.getDeploymentId());

            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove existed client %s", event.getAcid()));
            }
        }

        if (cce != null) {
            cce.setAcid(event.getAcid());
            cce.setDeploymentId(event.getDeploymentId());
            this.clientTableByAcid.put(cce.getAcid(), cce);
        } else {
            logger.warn(String.format("unable to find deployed client verticle %s %s",
                    event.getDeploymentId(),
                    event.getAcid()));
        }

        //forward logon event to other instance,
        //
        if (this.instances > 0) {
            ClientEvent logonForward = event.clone();
            logonForward.setEventType(ClientEvent.LOGON_FOWARD);
            for (int i = 0 ; i < this.instances; i++) {
                if (i == this.id) {
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format("skip forward logon event to self %d", this.id));
                    }
                    continue;
                }

                vertx.eventBus().send(ClientEvent.EVENT_ADDRESS_PREFIX + "." +i,
                        JSON.toJSONString(logonForward));
            }
        }
        //
        KafkaProducerRecord<String, String> logonEvent =
                new KafkaProducerRecordImpl<>(this.logonTopic,
                        event.getAcid(),
                        sessionInfoJson);
        this.kafkaProducer.write(logonEvent);
    }

    private void handleSocketClose(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle socket close %s %s",
                    event.getAcid(),
                    event.getDeploymentId()));
        }

        this.clientsCounter--;

        Client client = null;

        if (StringUtils.isNotEmpty(event.getDeploymentId())) {
            ClientVerticleEntity entity = this.clientTableByDepId.remove(event.getDeploymentId());
            if (entity != null) {
                client = entity.getClient();
            }
            if (logger.isInfoEnabled()) {
                logger.info(String.format("undeploy client verticle %s %s",
                        event.getAcid(),
                        event.getDeploymentId()));
            }
        }
        if (StringUtils.isNotEmpty(event.getAcid())) {
            ClientVerticleEntity entity = this.clientTableByAcid.remove(event.getAcid());
            if (entity != null) {
                client = entity.getClient();
            }
            this.redisClient.del(RedisHelper.generateOnlineKey(event.getAcid()), null);
            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove client %s %s",
                        event.getAcid(),
                        event.getDeploymentId()));
            }
        }

        if (client != null) {
            client.stop();
        }
    }

    private void handleClientVerticleStop(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle client verticle stop %s %s",
                    event.getAcid(),
                    event.getDeploymentId()));
        }
    }

    private void handleLogonForward(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle logon forward %s", event.getAcid()));
        }
        ClientVerticleEntity cce2 = this.clientTableByAcid.get(event.getAcid());

        if (cce2 != null) {
            vertx.undeploy(cce2.getDeploymentId());
            this.clientTableByDepId.remove(cce2.getDeploymentId());

            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove existed client %s", event.getAcid()));
            }
        }
    }

    private void handleNotificationAck(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle notification ack %s", event.getCommandObject().toJSONString()));
        }

        Notification notification = new Notification();
        notification.fromJSONObject(event.getCommandObject().getJSONObject(JsonCommand.NOTIFICATION));

        NotificationEntity notificationEntity = new NotificationEntity();
        notificationEntity.setTtl(0);
        notificationEntity.setNotification(notification);
        notificationEntity.setStatus(NotificationEntity.DELIVERED);
        notificationEntity.setCreateDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

        KafkaProducerRecord<String, String> kafkaProducerRecord =
                new KafkaProducerRecordImpl<>(this.notificationAckTopic,
                        notification.getTo(),
                        JSON.toJSONString(notificationEntity));

        this.kafkaProducer.write(kafkaProducerRecord);
    }

    private void notificationAskEventHandler(Message<String> msg) {
        String to = msg.body();
        if (this.clientTableByAcid.containsKey(to)) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("got notification target %s", to));
            }
            msg.reply(this.notificationEventAddress);
        }
    }

    private void notificationEventHandler(Message<String> msg) {
        Notification notification = JSON.parseObject(msg.body(), Notification.class);

        ClientVerticleEntity entity = this.clientTableByAcid.get(notification.getTo());
        if (entity == null) {
            logger.warn(String.format("%s is offline", notification.getTo()));
            return;
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JsonCommand.ACID, notification.getTo());
        jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.NOTIFICATION_CODE);
        jsonObject.put(JsonCommand.NOTIFICATION, notification.toJSONObject());

        JsonCommand jsonCommand = JsonCommand.create(Command.REQUEST, jsonObject);

        entity.getClient().writeCommand(jsonCommand, Command.REQUEST);

    }

    public void publishNotificationEventAddressOnOff(boolean on, String acid) {
        NotificationEventAddressBroadcast neab = new NotificationEventAddressBroadcast();
        neab.setOn(on);
        neab.setAcid(acid);
        neab.setEventAddress(this.notificationEventAddress);

        vertx.eventBus().publish(NotificationEventAddressBroadcast.EVENT_ADDRESS,
                JSON.toJSONString(neab));


    }

    private int id;
    private int port;
    private int maxClients;
    private int instances;
    private RedisOptions redisOptions;
    private String apiHost;
    private int apiPort;
    private Map<String, String> kafkaProducerConfig;
    private String notificationAckTopic;
    private String logonTopic;

    private RedisClient redisClient;

    private int clientsCounter;
    private int logonCounter;
    private HashMap<String, ClientVerticleEntity> clientTableByDepId;
    private HashMap<String, ClientVerticleEntity> clientTableByAcid;
    private String clientEventAddress;
    private String notificationEventAddress;

    private KafkaProducer<String, String> kafkaProducer;

    private Logger logger = LoggerFactory.getLogger(ClientServer.class);

    private class ClientVerticleEntity {

        public String getAcid() {
            return acid;
        }

        public void setAcid(String acid) {
            this.acid = acid;
        }

        public String getDeploymentId() {
            return deploymentId;
        }

        public void setDeploymentId(String deploymentId) {
            this.deploymentId = deploymentId;
        }

        public Client getClient() {
            return client;
        }

        public void setClient(Client client) {
            this.client = client;
        }

        private String acid;
        private String deploymentId;
        private Client client;
    }
}
