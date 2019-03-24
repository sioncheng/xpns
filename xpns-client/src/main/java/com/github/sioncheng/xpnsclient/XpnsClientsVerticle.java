package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSON;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

public class XpnsClientsVerticle extends AbstractVerticle {

    public XpnsClientsVerticle(int appId, int fromId, int toId, String prefix,
                               int clients, String remoteHost, int remotePort) {
        this.appId = appId;
        this.fromId = fromId;
        this.toId = toId;
        this.prefix = prefix;
        this.clients = clients;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.connected = 0;
        this.deployedClients = new HashMap<>();
        this.clientDeploymentEventAddress = String.format("%s_%d_%d_%d",
                ClientDeploymentEvent.EVENT_ADDRESS,
                appId,
                fromId,
                toId);
        this.clientActivationEventAddress = String.format("%s_%d_%d_%d",
                ClientActivationEvent.EVENT_ADDRESS,
                appId,
                fromId,
                toId);
    }

    @Override
    public void start() {
        //super.start();

        vertx.eventBus().consumer(clientDeploymentEventAddress, this::clientDeploymentEventHandler);
        vertx.eventBus().consumer(clientActivationEventAddress, this::clientActivationEventHandler);

        startConnections();
    }

    private void clientActivationEventHandler(Message<String> msg) {
        ClientActivationEvent event = JSON.parseObject(msg.body(), ClientActivationEvent.class);

        switch (event.getEventType()) {
            case ClientActivationEvent.LOGON_EVENT:
                connected++;
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("logon event %s", event.getAcid()));
                }
                break;
            case ClientActivationEvent.NOTIFICATION_EVENT:
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("notification event %s", event.getAcid()));
                }
                break;
            case ClientActivationEvent.CLOSE_EVENT:
                connected--;
                String acid = event.getAcid();
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("close event %s", acid));
                }

                String verticleId = this.deployedClients.remove(acid);
                if (StringUtils.isNotEmpty(verticleId)) {
                    vertx.undeploy(verticleId, result -> {
                        if (logger.isInfoEnabled()) {
                            logger.info(String.format("undepoy xpns client verticle %s %s %s"
                                    , Boolean.toString(result.succeeded()), acid, verticleId));
                        }

                        vertx.setTimer(5000 + System.currentTimeMillis() % 2000, new Handler<Long>() {
                            @Override
                            public void handle(Long tid) {
                                recreateClient(acid);
                                vertx.cancelTimer(tid);
                            }
                        });

                    });

                } else {
                    logger.warn(String.format("cant find deployed client verticle for %s", acid));
                }
                break;
        }
    }

    private void clientDeploymentEventHandler(Message<String> msg) {
        ClientDeploymentEvent event = JSON.parseObject(msg.body(), ClientDeploymentEvent.class);
        this.deployedClients.put(event.getAcid(), event.getVerticleId());

        if (logger.isInfoEnabled()) {
            logger.info(String.format("client deployment event %s %s",
                    event.getAcid(), event.getVerticleId()));
        }
    }

    private void startConnections() {
        for (int i = 0 ; i < clients; i++) {
            startConnect(fromId + i);
        }
    }

    private void startConnect(final int n) {
        NetClient netClient = vertx.createNetClient();
        netClient.connect(this.remotePort, this.remoteHost, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> netSocketAsyncResult) {
                if (netSocketAsyncResult.succeeded()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("connect to remote");
                    }

                    String clientId = generateClientId(n);

                    deployClientVerticle(clientId, netSocketAsyncResult.result());
                } else {
                    //
                    logger.warn(String.format("connect to remote failure: %s", netSocketAsyncResult.cause().getMessage()));

                    vertx.setTimer(10000 + System.currentTimeMillis() % 3000, tid -> {
                        logger.info("restartConnect");
                        vertx.cancelTimer(tid);
                        startConnect(n);
                    });
                }
            }
        });
    }

    private String generateClientId(int n) {
        StringBuilder sb = new StringBuilder();

        sb.append(this.appId);
        sb.append("_");

        for (int i = 0 ; i < 10; i++) {
            sb.append(this.prefix);
        }

        String sn = Integer.toString(n + fromId - 1);
        for (int i = 0 ; i < 10 - sn.length(); i++) {
            sb.append("0");
        }
        sb.append(sn);

        return sb.toString();
    }

    private void recreateClient(final String clientId) {
        NetClient netClient = vertx.createNetClient();
        netClient.connect(this.remotePort, this.remoteHost, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> netSocketAsyncResult) {
                if (netSocketAsyncResult.succeeded()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("connect to remote while recreate " + clientId);
                    }

                    deployClientVerticle(clientId, netSocketAsyncResult.result());
                } else {
                    //
                    logger.warn("connect to remote failure while recreate " + clientId);
                }
            }
        });
    }

    private void deployClientVerticle(String clientId, NetSocket netSocket) {
        XpnsClientVerticle clientVerticle =
                new XpnsClientVerticle(clientId,
                        netSocket,
                        clientActivationEventAddress);

        vertx.deployVerticle(clientVerticle, deployResult -> {
            if (deployResult.succeeded()) {
                ClientDeploymentEvent event =
                        new ClientDeploymentEvent(clientId, deployResult.result());
                vertx.eventBus().send(clientDeploymentEventAddress, JSON.toJSONString(event));
            } else {
                ///
                logger.warn(String.format("deploy xpns client verticle failure %s", clientId));
            }
        });
    }

    private int appId;
    private int fromId;
    private int toId;
    private String prefix;
    private int clients;
    private String remoteHost;
    private int remotePort;

    private int connected;
    private HashMap<String, String> deployedClients;
    private String clientDeploymentEventAddress;
    private String clientActivationEventAddress;

    private Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
