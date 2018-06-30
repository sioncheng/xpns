package com.github.sioncheng.xpnsserver.vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.redis.RedisOptions;

public class XpnsServer extends AbstractVerticle {

    public XpnsServer(XpnsServerConfig serverConfig) {

        this.serverConfig = serverConfig;
    }

    @Override
    public void start() {
        startClientServer();
        startApiServer();
    }

    @Override
    public void stop() {

    }

    private void startClientServer() {
        int maxClients = this.serverConfig.getMaxClients() / this.serverConfig.getClientInstances();

        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setHost(this.serverConfig.getRedisHost());
        redisOptions.setPort(this.serverConfig.getRedisPort());

        int i = 0;
        for (; i < this.serverConfig.getClientInstances() - 1; i++){
            ClientServerVerticle verticle = new ClientServerVerticle(i,
                    this.serverConfig.getClientPort(),
                    maxClients,
                    this.serverConfig.getClientInstances(),
                    redisOptions,
                    this.serverConfig.getApiHost(),
                    this.serverConfig.getApiPort());
            vertx.deployVerticle(verticle);
        }

        ClientServerVerticle verticle = new ClientServerVerticle(this.serverConfig.getClientInstances() - 1,
                this.serverConfig.getClientPort(),
                this.serverConfig.getMaxClients() - i * maxClients,
                this.serverConfig.getClientInstances(),
                redisOptions,
                this.serverConfig.getApiHost(),
                this.serverConfig.getApiPort());
        vertx.deployVerticle(verticle);
    }

    private void startApiServer() {

        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setHost(this.serverConfig.getRedisHost());
        redisOptions.setPort(this.serverConfig.getRedisPort());

        for (int i = 0 ; i < this.serverConfig.getApiInstances(); i++) {
            ApiServerVerticle apiServerVerticle = new ApiServerVerticle(i,
                    this.serverConfig.getApiPort(),
                    this.serverConfig.getApiHost(),
                    redisOptions);
            vertx.deployVerticle(apiServerVerticle);
        }
    }

    private XpnsServerConfig serverConfig;
}
