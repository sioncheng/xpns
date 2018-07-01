package com.github.sioncheng.xpnsserver.vertx;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.config.AppProperties;
import com.github.sioncheng.xpns.common.zk.Directories;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisOptions;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.Map;

public class XpnsServer extends AbstractVerticle {

    public XpnsServer(XpnsServerConfig serverConfig) {

        this.serverConfig = serverConfig;
    }

    @Override
    public void start() {
        startClientServer();
        startApiServer();
        startZookeeper();
    }

    @Override
    public void stop() {

    }

    private void startClientServer() {
        int maxClients = this.serverConfig.getMaxClients() / this.serverConfig.getClientInstances();

        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setHost(this.serverConfig.getRedisHost());
        redisOptions.setPort(this.serverConfig.getRedisPort());

        Map<String, String> kafkaProducerConfig = AppProperties.getPropertiesWithPrefix("kafka.producer.");
        String notifcationAckTopic = AppProperties.getString("kafka-ack-topic");

        int i = 0;
        for (; i < this.serverConfig.getClientInstances() - 1; i++){
            ClientServerVerticle verticle = new ClientServerVerticle(i,
                    this.serverConfig.getClientPort(),
                    maxClients,
                    this.serverConfig.getClientInstances(),
                    redisOptions,
                    this.serverConfig.getApiHost(),
                    this.serverConfig.getApiPort(),
                    kafkaProducerConfig,
                    notifcationAckTopic);
            vertx.deployVerticle(verticle);
        }

        ClientServerVerticle verticle = new ClientServerVerticle(this.serverConfig.getClientInstances() - 1,
                this.serverConfig.getClientPort(),
                this.serverConfig.getMaxClients() - i * maxClients,
                this.serverConfig.getClientInstances(),
                redisOptions,
                this.serverConfig.getApiHost(),
                this.serverConfig.getApiPort(),
                kafkaProducerConfig,
                notifcationAckTopic);
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

    private void startZookeeper() {
        vertx.executeBlocking(this::registerService, this::registerServiceHandler);
    }

    private void registerService(Future f) {
        try {
            String zkServers = AppProperties.getString("zookeeper.servers");

            this.zooKeeper = new ZooKeeper(zkServers, 5000, null);

            String root = Directories.XPNS_SERVER_ROOT;
            if (this.zooKeeper.exists(root, false) == null) {
                this.zooKeeper.create(root, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            String apiServices = root + "/" + Directories.API_SERVICES;
            if (this.zooKeeper.exists(apiServices, false) == null) {
                this.zooKeeper.create(apiServices, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            String servicePath = apiServices + "/" +
                    this.serverConfig.getApiHost() + ":" + this.serverConfig.getApiPort();
            this.zooKeeper.create(servicePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            f.complete();
        } catch (Exception ex) {
            logger.error("discover services error", ex);
            f.fail(ex);
        }
    }

    private void registerServiceHandler(AsyncResult result) {
        logger.info(String.format("register server handler %s", result.succeeded()));
    }

    private XpnsServerConfig serverConfig;

    private ZooKeeper zooKeeper;

    private static final Logger logger = LoggerFactory.getLogger(XpnsServer.class);
}
