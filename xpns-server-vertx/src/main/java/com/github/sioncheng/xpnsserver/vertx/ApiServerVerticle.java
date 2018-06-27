package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;

public class ApiServerVerticle extends AbstractVerticle {

    public ApiServerVerticle(int id, int port, String host) {
        this.id = id;
        this.port = port;
        this.host = host;
        this.clientEventAddressMap = new HashMap<>();
    }

    @Override
    public void start() throws Exception {

        httpServer = vertx.createHttpServer();
        httpServer.requestHandler(this::httpRequestHandler);
        httpServer.listen(this.port, this.host, result -> {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("api server listen result %s", Boolean.toString(result.succeeded())));
            }
        });

        vertx.eventBus().consumer(NotificationEventAddressBroadcast.EVENT_ADDRESS, this::neabEventHandler);

        super.start();
    }

    @Override
    public void stop() throws Exception {


        super.stop();
    }

    private void httpRequestHandler(HttpServerRequest request) {

    }

    private void neabEventHandler(Message<String> msg) {
        NotificationEventAddressBroadcast neab = JSON.parseObject(msg.body(),
                NotificationEventAddressBroadcast.class);

        if (neab.isOn()) {
            clientEventAddressMap.put(neab.getAcid(), neab.getEventAddress());
        } else {
            clientEventAddressMap.remove(neab.getAcid());
        }
    }

    private int id;
    private int port;
    private String host;

    private HttpServer httpServer;

    private HashMap<String, String> clientEventAddressMap;

    private static final Logger logger = LoggerFactory.getLogger(ApiServerVerticle.class);
}
