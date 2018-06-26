package com.github.sioncheng.xpnsserver;


import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MainApp {

    public static void main(String[] args) throws Exception {


        ServerProperties.init();

        RedisSessionManager redisSessionManager = new RedisSessionManager(ServerProperties.getString("redis.host")
                , ServerProperties.getInt("redis.port"));

        KafkaNotificationConsumerConfig kafkaConsumerConfig = new KafkaNotificationConsumerConfig();
        kafkaConsumerConfig.setBootstrapServer(ServerProperties.getString("kafka.bootstrap.server"));
        kafkaConsumerConfig.setGroupId(ServerProperties.getString("kafka.group.id"));
        kafkaConsumerConfig.setEnableAutoCommit(ServerProperties.getBoolean("kafka.enable.auto.commit"));
        kafkaConsumerConfig.setSessionTimeoutMS(ServerProperties.getInt("kafka.session.timeout.ms"));
        kafkaConsumerConfig.setAutoCommitIntervalMS(ServerProperties.getInt("kafka.auto.commit.interval.ms"));
        kafkaConsumerConfig.setAutoOffsetReset(ServerProperties.getString("kafka.auto.offset.reset"));
        kafkaConsumerConfig.setKeyDeserializer(ServerProperties.getString("kafka.key.deserializer"));
        kafkaConsumerConfig.setValueDeserializer(ServerProperties.getString("kafka.value.deserializer"));

        KafkaNotificationManager kafkaNotificationManager = new KafkaNotificationManager(kafkaConsumerConfig);


        XpnsServerConfig xpnsServerConfig = new XpnsServerConfig();
        xpnsServerConfig.setClientPort(ServerProperties.getInt("server.port"));
        xpnsServerConfig.setMaxClients(ServerProperties.getInt("server.max.clients"));
        xpnsServerConfig.setApiPort(ServerProperties.getInt("server.api.port"));
        xpnsServerConfig.setApiServer(ServerProperties.getString("server.api.host"));
        xpnsServerConfig.setNettyEventLoopGroupThreadsForClient(ServerProperties.getInt("server.client.netty.threads"));
        xpnsServerConfig.setNettyEventLoopGroupThreadsForApi(ServerProperties.getInt("server.api.netty.threads"));
        xpnsServerConfig.setWorkerThreads(ServerProperties.getInt("server.worker.threads"));

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
