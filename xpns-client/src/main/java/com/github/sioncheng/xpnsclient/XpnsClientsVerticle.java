package com.github.sioncheng.xpnsclient;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;

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
        this.xpnsClients = new HashMap<>();
        this.connected = 0;
    }

    @Override
    public void start() throws Exception {
        //super.start();

        connectToRemote();
    }

    private void connectToRemote() {
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
                    connectToRemote();
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

    private HashMap<String, XpnsClientVerticle> xpnsClients;
    private int connected;
    private Logger logger = LoggerFactory.getLogger(XpnsClientVerticle.class);
}
