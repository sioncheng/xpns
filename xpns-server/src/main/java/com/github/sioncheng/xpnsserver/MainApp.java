package com.github.sioncheng.xpnsserver;


import com.github.sioncheng.xpns.common.config.AppProperties;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MainApp {

    public static void main(String[] args) throws Exception {


        AppProperties.init();

        RedisSessionManager redisSessionManager = new RedisSessionManager(AppProperties.getString("redis.host")
                , AppProperties.getInt("redis.port"));

        KafkaNotificationConsumerConfig kafkaConsumerConfig = new KafkaNotificationConsumerConfig();
        kafkaConsumerConfig.setBootstrapServer(AppProperties.getString("kafka.bootstrap.server"));
        kafkaConsumerConfig.setGroupId(AppProperties.getString("kafka.group.id"));
        kafkaConsumerConfig.setEnableAutoCommit(AppProperties.getBoolean("kafka.enable.auto.commit"));
        kafkaConsumerConfig.setSessionTimeoutMS(AppProperties.getInt("kafka.session.timeout.ms"));
        kafkaConsumerConfig.setAutoCommitIntervalMS(AppProperties.getInt("kafka.auto.commit.interval.ms"));
        kafkaConsumerConfig.setAutoOffsetReset(AppProperties.getString("kafka.auto.offset.reset"));
        kafkaConsumerConfig.setKeyDeserializer(AppProperties.getString("kafka.key.deserializer"));
        kafkaConsumerConfig.setValueDeserializer(AppProperties.getString("kafka.value.deserializer"));

        KafkaNotificationManager kafkaNotificationManager = new KafkaNotificationManager(kafkaConsumerConfig);


        XpnsServerConfig xpnsServerConfig = new XpnsServerConfig();
        xpnsServerConfig.setClientPort(AppProperties.getInt("server.port"));
        xpnsServerConfig.setMaxClients(AppProperties.getInt("server.max.clients"));
        xpnsServerConfig.setApiPort(AppProperties.getInt("server.api.port"));
        xpnsServerConfig.setApiServer(AppProperties.getString("server.api.host"));
        xpnsServerConfig.setNettyEventLoopGroupThreadsForClient(AppProperties.getInt("server.client.netty.threads"));
        xpnsServerConfig.setNettyEventLoopGroupThreadsForApi(AppProperties.getInt("server.api.netty.threads"));
        xpnsServerConfig.setWorkerThreads(AppProperties.getInt("server.worker.threads"));

        XpnsServer xpnsServer = new XpnsServer(xpnsServerConfig,
                redisSessionManager,
                kafkaNotificationManager);

        xpnsServer.start();

        new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("...");
            }
        }, 60, 60, TimeUnit.SECONDS);
    }
}
