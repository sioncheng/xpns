package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

public class ClientServerVerticle extends AbstractVerticle {

    public ClientServerVerticle(int id, int port, int maxClients, int instances,
                                RedisOptions redisOptions, String apiHost, int apiPort) {
        this.id = id;
        this.port = port;
        this.maxClients = maxClients;
        this.instances = instances;
        this.redisOptions = redisOptions;
        this.apiHost = apiHost;
        this.apiPort = apiPort;
        this.clientsCounter = 0;
        this.logonCounter = 0;
        this.clientVerticleTable1 = new HashMap<>();
        this.clientVerticleTable2 = new HashMap<>();
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

        vertx.eventBus().consumer(this.clientEventAddress, this::clientEventHandler);
        vertx.eventBus().consumer(NotificationAskEvent.EVENT_ADDRESS, this::notificationAskEventHandler);
        vertx.eventBus().consumer(this.notificationEventAddress, this::notificationEventHandler);

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

        ClientVerticle clientVerticle = new ClientVerticle(this.id, this.clientEventAddress, netSocket);
        vertx.deployVerticle(clientVerticle, result -> {
            if (result.succeeded()) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("deploy client success %s", result.result()));
                    ClientVerticleEntity cce = new ClientVerticleEntity();
                    cce.setAcid("");
                    cce.setDeploymentId(result.result());
                    cce.setClientVerticle(clientVerticle);
                    this.clientVerticleTable1.put(result.result(), cce);
                }
            } else {
                logger.warn("deploy client verticle failure");
            }
        });

    }

    private void clientEventHandler(Message<String> msg) {
        ClientEvent event = JSON.parseObject(msg.body(), ClientEvent.class);

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
                break;
            case ClientEvent.LOGON_FOWARD:
                this.handleLogonForward(event);
                break;
            default:
                logger.warn(String.format("unknown event %s", msg));
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

        final String onlineKey = RedisHelper.generateOnlineKey(sessionInfo.getAcid());
        this.redisClient.set(onlineKey,
                JSON.toJSONString(sessionInfo),
                result ->{
                    if (result.succeeded()) {
                        this.redisClient.expire(onlineKey, 3600, null);
                    } else {
                        logger.warn(String.format("set online info to redis failure %s", onlineKey));
                    }
                });

        ClientVerticleEntity cce = this.clientVerticleTable1.get(event.getDeploymentId());
        ClientVerticleEntity cce2 = this.clientVerticleTable2.get(event.getAcid());

        //if there is the same client, close it.
        if (cce2 != null) {
            vertx.undeploy(cce2.getDeploymentId());
            this.clientVerticleTable1.remove(cce2.getDeploymentId());

            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove existed client %s", event.getAcid()));
            }
        }

        if (cce != null) {
            cce.setAcid(event.getAcid());

            cce2 = new ClientVerticleEntity();
            cce2.setAcid(event.getAcid());
            cce2.setDeploymentId(event.getDeploymentId());
            cce2.setClientVerticle(cce.getClientVerticle());
            this.clientVerticleTable2.put(cce2.getAcid(), cce2);
        } else {
            logger.warn(String.format("unable to find deployed client verticle", event.getDeploymentId()));
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
    }

    private void handleSocketClose(ClientEvent event) {
        this.clientsCounter--;
        if (StringUtils.isNotEmpty(event.getDeploymentId())) {
            this.clientVerticleTable1.remove(event.getDeploymentId());
            vertx.undeploy(event.getDeploymentId());
        }
        if (StringUtils.isNotEmpty(event.getAcid())) {
            this.clientVerticleTable2.remove(event.getAcid());


            this.redisClient.del(RedisHelper.generateOnlineKey(event.getAcid()), null);
        }

    }

    private void handleLogonForward(ClientEvent event) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("handle logon forward %s", event.getAcid()));
        }
        ClientVerticleEntity cce2 = this.clientVerticleTable2.get(event.getAcid());

        if (cce2 != null) {
            vertx.undeploy(cce2.getDeploymentId());
            this.clientVerticleTable1.remove(cce2.getDeploymentId());

            if (logger.isInfoEnabled()) {
                logger.info(String.format("remove existed client %s", event.getAcid()));
            }
        }
    }

    private void notificationAskEventHandler(Message<String> msg) {
        String to = msg.body();
        if (this.clientVerticleTable2.containsKey(to)) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("got notification target %s", to));
            }
            msg.reply(this.notificationEventAddress);
        }
    }

    private void notificationEventHandler(Message<String> msg) {

    }

    private int id;
    private int port;
    private int maxClients;
    private int instances;
    private RedisOptions redisOptions;
    private String apiHost;
    private int apiPort;

    private RedisClient redisClient;

    private int clientsCounter;
    private int logonCounter;
    private HashMap<String, ClientVerticleEntity> clientVerticleTable1;
    private HashMap<String, ClientVerticleEntity> clientVerticleTable2;
    private String clientEventAddress;
    private String notificationEventAddress;

    private Logger logger = LoggerFactory.getLogger(ClientServerVerticle.class);

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

        public ClientVerticle getClientVerticle() {
            return clientVerticle;
        }

        public void setClientVerticle(ClientVerticle clientVerticle) {
            this.clientVerticle = clientVerticle;
        }

        private String acid;
        private String deploymentId;
        private ClientVerticle clientVerticle;
    }
}
