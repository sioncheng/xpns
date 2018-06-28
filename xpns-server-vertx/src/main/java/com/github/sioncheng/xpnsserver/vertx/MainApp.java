package com.github.sioncheng.xpnsserver.vertx;

import com.github.sioncheng.xpns.common.config.ServerProperties;
import io.vertx.core.Vertx;

import java.io.IOException;

public class MainApp {

    public static void main(String[] args) throws IOException {

        ServerProperties.init();

        XpnsServerConfig serverConfig = new XpnsServerConfig();

        serverConfig.setClientPort(ServerProperties.getInt("server.port"));
        serverConfig.setClientInstances(ServerProperties.getInt("server.client.vertx.instances"));
        serverConfig.setMaxClients(ServerProperties.getInt("server.max.clients"));

        serverConfig.setApiPort(ServerProperties.getInt("server.api.port"));
        serverConfig.setApiHost(ServerProperties.getString("server.api.host"));
        serverConfig.setApiInstances(ServerProperties.getInt("server.api.vertx.instances"));

        serverConfig.setWorkerThreads(ServerProperties.getInt("server.worker.threads"));

        serverConfig.setRedisHost(ServerProperties.getString("redis.host"));
        serverConfig.setRedisPort(ServerProperties.getInt("redis.port"));

        XpnsServer xpnsServer = new XpnsServer(serverConfig);

        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(xpnsServer);

        System.in.read();
    }
}
