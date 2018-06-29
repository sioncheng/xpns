package com.github.sioncheng.xpns.dispatcher;

import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.*;

public class MainApp {

    public static void main(String[] args) throws Exception {

        Map<String, String> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my_group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        Vertx vertx = Vertx.vertx();

        Set<String> topics = new HashSet<>();
        topics.add("xpns-notification");

        KafkaConsumerVerticle kafkaConsumerVerticle = new KafkaConsumerVerticle(config, topics);

        vertx.deployVerticle(kafkaConsumerVerticle);

        System.in.read();

    }
}
