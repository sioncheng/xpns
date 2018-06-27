package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

public class ClientServerVerticle extends AbstractVerticle {

    public ClientServerVerticle(int id, int port, int maxClients) {
        this.id = id;
        this.port = port;
        this.maxClients = maxClients;
        this.clientsCounter = 0;
        this.logonCounter = 0;
        this.clientVerticleTable1 = new HashMap<>();
        this.clientVerticleTable2 = new HashMap<>();
        this.clientEventAddress = ClientEvent.EVENT_ADDRESS + "." + this.id;
        this.notificationEventAddress = NotificationEvent.EVENT_ADDRESS_PREFIX + "." + this.id;
    }

    @Override
    public void start() throws Exception {

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
                this.logonCounter++;
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("client logon %s %d", event.getAcid(), this.logonCounter));
                }
                ClientVerticleEntity cce = this.clientVerticleTable1.get(event.getDeploymentId());
                if (cce != null) {
                    cce.setAcid(event.getAcid());

                    ClientVerticleEntity cce2 = new ClientVerticleEntity();
                    cce2.setAcid(event.getAcid());
                    cce2.setDeploymentId(event.getDeploymentId());
                    cce2.setClientVerticle(cce.getClientVerticle());
                    this.clientVerticleTable2.put(cce2.getAcid(), cce2);
                } else {
                    logger.warn(String.format("unable to find deployed client verticle", event.getDeploymentId()));
                }

                break;
            case ClientEvent.SOCKET_CLOSE:
                this.clientsCounter--;
                if (StringUtils.isNotEmpty(event.getDeploymentId())) {
                    this.clientVerticleTable1.remove(event.getDeploymentId());
                    vertx.undeploy(event.getDeploymentId());
                }
                if (StringUtils.isNotEmpty(event.getAcid())) {
                    this.clientVerticleTable2.remove(event.getDeploymentId());
                }
                break;

            case ClientEvent.STOP:

                break;
            default:
                logger.warn(String.format("unknown event %s", msg));
                break;

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
