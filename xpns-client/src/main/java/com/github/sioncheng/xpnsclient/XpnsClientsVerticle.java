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
    }

    @Override
    public void start() {
        //super.start();

        vertx.eventBus().consumer(ClientActivationEvent.EVENT_ADDRESS, this::clientActivationEventHandler);

        startConnections();
    }

    private void clientActivationEventHandler(Message<String> msg) {
        ClientActivationEvent event = JSON.parseObject(msg.body(), ClientActivationEvent.class);

        switch (event.getEventType()) {
            case ClientActivationEvent.LOGON_EVENT:
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
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("close event %s", event.getAcid()));
                }
                break;
        }
    }

    private void startConnections() {
        NetClient netClient = vertx.createNetClient();
        netClient.connect(this.remotePort, this.remoteHost, new Handler<AsyncResult<NetSocket>>() {
            @Override
            public void handle(AsyncResult<NetSocket> netSocketAsyncResult) {
                if (netSocketAsyncResult.succeeded()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("connect to remote");
                    }

                    connected++;
                    XpnsClientVerticle clientVerticle =
                            new XpnsClientVerticle(generateClientId(connected), netSocketAsyncResult.result());
                    vertx.deployVerticle(clientVerticle);
                } else {
                    //
                    logger.warn("connect to remote failure");
                }

                if (connected < clients) {
                    startConnections();
                } else {
                    logger.info(String.format("reach the max clients %d", clients));
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

    private int appId;
    private int fromId;
    private int toId;
    private String prefix;
    private int clients;
    private String remoteHost;
    private int remotePort;

    private int connected;
    private Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
