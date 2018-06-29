package com.github.sioncheng.xpnsserver.vertx;

import com.github.sioncheng.xpns.common.config.AppProperties;
import io.vertx.core.Vertx;

import java.io.IOException;

public class MainApp {

    public static void main(String[] args) throws IOException {

        AppProperties.init();

        XpnsServerConfig serverConfig = new XpnsServerConfig();

        serverConfig.setClientPort(AppProperties.getInt("server.port"));
        serverConfig.setClientInstances(AppProperties.getInt("server.client.vertx.instances"));
        serverConfig.setMaxClients(AppProperties.getInt("server.max.clients"));

        serverConfig.setApiPort(AppProperties.getInt("server.api.port"));
        serverConfig.setApiHost(AppProperties.getString("server.api.host"));
        serverConfig.setApiInstances(AppProperties.getInt("server.api.vertx.instances"));

        serverConfig.setWorkerThreads(AppProperties.getInt("server.worker.threads"));

        serverConfig.setRedisHost(AppProperties.getString("redis.host"));
        serverConfig.setRedisPort(AppProperties.getInt("redis.port"));

        XpnsServer xpnsServer = new XpnsServer(serverConfig);

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(xpnsServer);

        System.in.read();
    }
}
