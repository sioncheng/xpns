package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;

import java.io.IOException;
import java.util.HashMap;

public class MainApp {

    public static void main(String[] args) throws IOException {

        HashMap<String, String> clientNotificationEventAddressMap = new HashMap<>();

        Vertx vertx = Vertx.vertx();

        for (int i = 0 ; i < 2; i++) {
            ClientServerVerticle verticle = new ClientServerVerticle(i,8080, 10);
            vertx.deployVerticle(verticle);
        }

        /*
        for (int i = 0 ; i < 2; i++) {
            ApiServerVerticle apiServerVerticle = new ApiServerVerticle(i,9090, "0.0.0.0");
            vertx.deployVerticle(apiServerVerticle);
        }*/

        vertx.eventBus().consumer(NotificationEventAddressBroadcast.EVENT_ADDRESS, msg -> {
            String msgBody = (String)msg.body();
            NotificationEventAddressBroadcast neab = JSON.parseObject(msgBody, NotificationEventAddressBroadcast.class);

            System.out.println(msgBody);
            if (neab.isOn()) {
                clientNotificationEventAddressMap.put(neab.getAcid(), neab.getEventAddress());
            } else {
                clientNotificationEventAddressMap.remove(neab.getAcid());
            }
        });


        System.in.read();

        Notification notification = new Notification();
        notification.setTo("1_aaaaaaaaaa0000000001");
        notification.setTitle("title");
        notification.setBody("body");
        notification.setExt(new JSONObject());

        String eventAddress = clientNotificationEventAddressMap.get("1_aaaaaaaaaa0000000001");
        vertx.eventBus().send(eventAddress, JSON.toJSONString(notification));

        System.in.read();
    }
}
