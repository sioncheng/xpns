package com.github.sioncheng.xpnsserver;


import com.github.sioncheng.xpns.common.config.AppProperties;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MainApp {

    public static void main(String[] args) throws Exception {


        AppProperties.init();

        RedisSessionManagerGroup redisSessionManager = new RedisSessionManagerGroup(AppProperties.getInt("server.worker.threads"),
                AppProperties.getString("redis.host"),
                AppProperties.getInt("redis.port"),
                AppProperties.getInt("redis.clients.max.total"),
                AppProperties.getInt("redis.clients.max.idle"));

        Map<String, String> kafkaProducerConfig = AppProperties.getPropertiesWithPrefix("kafka.producer.");
        Properties properties = new Properties();
        for (Map.Entry<String, String> entry :
                kafkaProducerConfig.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }

        KafkaProducerManager kafkaNotificationManager = new KafkaProducerManager(properties,
                AppProperties.getString("kafka-ack-topic"),
                AppProperties.getString("kafka-logon-topic"));


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
                kafkaNotificationManager,
                AppProperties.getString("zookeeper.servers"));

        xpnsServer.start();

        int r = System.in.read();
        while (r != (int)'q') {
            r = System.in.read();
        }
    }
}
