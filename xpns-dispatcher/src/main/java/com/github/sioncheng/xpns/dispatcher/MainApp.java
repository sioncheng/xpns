package com.github.sioncheng.xpns.dispatcher;

import com.github.sioncheng.xpns.common.config.AppProperties;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.*;

public class MainApp {

    public static void main(String[] args) throws Exception {

        AppProperties.init();

        Map<String, String> config = AppProperties.getPropertiesWithPrefix("kafka.consumer.");


        Vertx vertx = Vertx.vertx();

        Set<String> topics = new HashSet<>();
        topics.add("xpns-notification");

        KafkaConsumerVerticle kafkaConsumerVerticle = new KafkaConsumerVerticle(config,
                topics,
                AppProperties.getString("zookeeper.servers"));

        vertx.deployVerticle(kafkaConsumerVerticle);

        System.in.read();

    }
}
