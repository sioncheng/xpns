package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.UUID;

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
        String url = request.uri();
        if ("/notification".equals(url)) {
            this.handelNotification(request);
        } else if ("client".equals(url)) {
            this.handleClient(request);
        } else {
            this.handleUnknown(request);
        }
    }

    private void handelNotification(HttpServerRequest request) {
        request.bodyHandler(buffer -> {
            String s = new String(buffer.getBytes());
            Notification notification = JSON.parseObject(s, Notification.class);

            String uuid = (UUID.randomUUID()).toString();
            notification.setUniqId(uuid);

            String targetEventAddress = this.clientEventAddressMap.get(notification.getTo());
            if (StringUtils.isNotEmpty(targetEventAddress)) {
                vertx.eventBus().send(targetEventAddress, JSON.toJSONString(notification));
            }

            JSONObject responseJson = new JSONObject();
            responseJson.put("result", "ok");
            responseJson.put("messageId", notification.getUniqId());

            this.writeResponse(request.response(), responseJson);

        });
    }

    private void handleClient(HttpServerRequest request) {

    }

    private void handleUnknown(HttpServerRequest request) {

    }

    private void writeResponse(HttpServerResponse response, JSONObject responseJson) {
        byte[] payload = responseJson.toJSONString().getBytes();
        response.setStatusCode(200);
        response.headers().add("Content-Type", "application/json")
                .add("Content-Length", String.valueOf(payload.length));
        response.write(Buffer.buffer(payload));
        response.end();
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
