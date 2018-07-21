package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
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

import java.util.*;

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

        vertx.setPeriodic(MILLIS_OF_10_MINUTES, this::scanClients);

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

        netSocket.pause();

        Client client = new Client(this, netSocket);

        vertx.deployVerticle(client);
    }

    private void clientEventMessageHandler(Message<String> msg) {
        ClientEvent event = JSON.parseObject(msg.body(), ClientEvent.class);

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client event handler %s %s", event.getAcid(), event.getEventType()));
        }
        switch (event.getEventType()) {
            case ClientEvent.START:
                this.handleStart(event);
                break;
            case ClientEvent.LOGON:
                this.handleLogon(event);
                break;
            case ClientEvent.SOCKET_CLOSE:
                this.handleSocketClose(event);
                break;
            case ClientEvent.STOP:
                this.handleClientStop(event);
                break;
            case ClientEvent.LOGON_FORWARD:
                this.handleLogonForward(event);
                break;
            case ClientEvent.ACK:
                this.handleNotificationAck(event);
                break;
            case ClientEvent.HEART_BEAT:
                this.handleClientHeartbeat(event);
                break;
            default:
                logger.warn(String.format("unknown event %s", event.getEventType()));
                break;

        }
    }


    public void clientEventHandler(ClientEvent event) {
        vertx.eventBus().send(this.clientEventAddress, JSON.toJSONString(event));
    }

    private void handleStart(ClientEvent event) {

        ClientVerticleEntity cce = new ClientVerticleEntity();
        cce.setAcid("");
        cce.setDeploymentId(event.getDeploymentId());
        cce.setActiveDateTime(System.currentTimeMillis());
        this.clientTableByDepId.put(cce.getDeploymentId(), cce);
    }

    private void handleLogon(ClientEvent event) {
        this.logonCounter++;
        if (logger.isInfoEnabled()) {
            logger.info(String.format("client logon %s %d", event.getAcid(), this.logonCounter));
        }

        this.setSessionInfo(event.getAcid());

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
            cce.setActiveDateTime(System.currentTimeMillis());

            this.clientTableByAcid.put(cce.getAcid(), cce);
        } else {
            logger.warn(String.format("unable to find deployed client verticle %s %s",
                    event.getDeploymentId(),
                    event.getAcid()));

            cce = new ClientVerticleEntity();
            cce.setAcid(event.getAcid());
            cce.setDeploymentId(event.getDeploymentId());
            cce.setActiveDateTime(System.currentTimeMillis());

            this.clientTableByDepId.put(cce.getDeploymentId(), cce);

            this.clientTableByAcid.put(cce.getAcid(), cce);
        }

        //forward logon event to other instance,
        //
        if (this.instances > 0) {
            ClientEvent logonForward = event.clone();
            logonForward.setEventType(ClientEvent.LOGON_FORWARD);
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
        SessionInfo sessionInfo = new SessionInfo();
        sessionInfo.setAcid(event.getAcid());
        sessionInfo.setServer(this.apiHost);
        sessionInfo.setPort(this.apiPort);

        KafkaProducerRecord<String, String> logonEvent =
                new KafkaProducerRecordImpl<>(this.logonTopic,
                        event.getAcid(),
                        JSON.toJSONString(sessionInfo));
        this.kafkaProducer.write(logonEvent);
    }

    private void handleSocketClose(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle socket close %s %s",
                    event.getAcid(),
                    event.getDeploymentId()));
        }

        this.clientsCounter--;


        if (StringUtils.isNotEmpty(event.getDeploymentId())) {
            this.clientTableByDepId.remove(event.getDeploymentId());
            if (logger.isInfoEnabled()) {
                logger.info(String.format("undeploy client verticle %s %s",
                        event.getAcid(),
                        event.getDeploymentId()));
            }
            vertx.undeploy(event.getDeploymentId());
        }
        if (StringUtils.isNotEmpty(event.getAcid())) {
            this.clientTableByAcid.remove(event.getAcid());
            this.redisClient.del(RedisHelper.generateOnlineKey(event.getAcid()), null);
            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove client %s %s",
                        event.getAcid(),
                        event.getDeploymentId()));
            }

            this.removeSessionInfo(event.getAcid());
        }
    }

    private void handleClientStop(ClientEvent event) {
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

    private void handleClientHeartbeat(ClientEvent event) {
        logger.info(String.format("handle client heart beat %d %s", Thread.currentThread().getId(),
                Thread.currentThread().getName()));

        ClientVerticleEntity entity = this.clientTableByAcid.get(event.getAcid());
        if (entity != null) {
            entity.setActiveDateTime(System.currentTimeMillis());
            this.setSessionInfo(entity.getAcid());
        } else {
            logger.warn(String.format("cant find client for %s during heartbeat", entity.getAcid()));
        }
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

        if (logger.isInfoEnabled()) {
            logger.info(String.format("sent notification command to %s", entity.getDeploymentId()));
        }

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(JsonCommand.ACID, notification.getTo());
        jsonObject.put(JsonCommand.COMMAND_CODE, JsonCommand.NOTIFICATION_CODE);
        jsonObject.put(JsonCommand.NOTIFICATION, notification.toJSONObject());

        vertx.eventBus().send(entity.getDeploymentId(), jsonObject.toJSONString());
    }

    public void publishNotificationEventAddressOnOff(boolean on, String acid) {
        NotificationEventAddressBroadcast neab = new NotificationEventAddressBroadcast();
        neab.setOn(on);
        neab.setAcid(acid);
        neab.setEventAddress(this.notificationEventAddress);

        vertx.eventBus().publish(NotificationEventAddressBroadcast.EVENT_ADDRESS,
                JSON.toJSONString(neab));


    }


    private void setSessionInfo(String acid) {
        //set to redis
        SessionInfo sessionInfo = new SessionInfo();
        sessionInfo.setAcid(acid);
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
    }

    private void removeSessionInfo(String acid) {

        final String onlineKey = RedisHelper.generateOnlineKey(acid);
        this.redisClient.del(onlineKey,
                result ->{
                    if (result.succeeded()) {
                        this.redisClient.expire(onlineKey, 3600, null);
                    } else {
                        logger.warn(String.format("del online info to redis failure %s", onlineKey));
                    }
                });
    }

    private void scanClients(long l) {
        logger.info(String.format("scan clients %d %s", Thread.currentThread().getId(),
                Thread.currentThread().getName()));

        if (this.clientTableByAcid == null) {
            return;
        }

        List<Map.Entry<String, ClientVerticleEntity>> zombies =
                new ArrayList<>();

        long now = System.currentTimeMillis();
        for (Map.Entry<String, ClientVerticleEntity> kv :
                this.clientTableByAcid.entrySet()) {
            long ts = now - kv.getValue().getActiveDateTime();
            if (ts > MILLIS_OF_10_MINUTES) {
                zombies.add(kv);
            }
        }

        zombies.forEach(kv -> {
            //this.removeSessionInfo(kv.getKey());
            //kv.getValue().getClient().stop();
            vertx.undeploy(kv.getValue().getDeploymentId());
        });
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

    private static final long MILLIS_OF_10_MINUTES = 10 * 60 * 1000L;

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

        public long getActiveDateTime() {
            return activeDateTime;
        }

        public void setActiveDateTime(long activeDateTime) {
            this.activeDateTime = activeDateTime;
        }

        private String acid;
        private String deploymentId;
        private long activeDateTime;
    }
}
