package com.github.sioncheng.xpnsclient;

import com.alibaba.fastjson.JSON;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        this.netClient = vertx.createNetClient();

        for (int i = 0 ; i < clients; i++) {
            String clientId = generateClientId(fromId + i);
            deployClientVerticle(clientId);
        }
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

    private void deployClientVerticle(String clientId) {
        XpnsClientVerticle clientVerticle =
                new XpnsClientVerticle(this.netClient, this.remoteHost, this.remotePort, clientId, clientActivationEventAddress);

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
    private NetClient netClient;

    private int connected;
    private HashMap<String, String> deployedClients;
    private String clientDeploymentEventAddress;
    private String clientActivationEventAddress;

    private Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
