package com.github.sioncheng.xpns.dispatcher;

import com.github.sioncheng.xpns.common.config.AppProperties;
import io.vertx.core.Vertx;

import java.util.*;

public class MainApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        AppProperties.init();

        Map<String, String> config = AppProperties.getPropertiesWithPrefix("kafka.consumer.");


        Vertx vertx = Vertx.vertx();

        KafkaEsVerticle kafkaEsVerticle = new KafkaEsVerticle();
        vertx.deployVerticle(kafkaEsVerticle);

//        Set<String> topicsAck = new HashSet<>();
//        topicsAck.add(AppProperties.getString("kafka-ack-topic"));
//        KafkaAckVerticle ackVerticle = new KafkaAckVerticle(config, topicsAck);
//        vertx.deployVerticle(ackVerticle);

        Set<String> topics = new HashSet<>();
        topics.add(AppProperties.getString("kafka-notification-topic"));
        topics.add(AppProperties.getString("kafka-ack-topic"));

        KafkaNotificationVerticle kafkaNotificationVerticle = new KafkaNotificationVerticle(config,
                topics,
                AppProperties.getString("zookeeper.servers"));

        vertx.deployVerticle(kafkaNotificationVerticle);

        System.in.read();

    }
}
