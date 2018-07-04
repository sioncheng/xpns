package com.github.sioncheng.xpns.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import com.github.sioncheng.xpns.common.zk.Directories;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;

public class KafkaNotificationVerticle extends AbstractVerticle implements Watcher {

    public KafkaNotificationVerticle(Map<String, String> config, Set<String> topics, String zkServers) {
        this.config = config;
        this.topics = topics;
        this.zkServers = zkServers;
        this.xpnsServicesEventAddress = "xpns.services." + this.hashCode();
        this.rand = new Random();
        this.stringArrayClazz = (new String[]{}).getClass();
    }

    @Override
    public void start() throws Exception {

        //startConsumer();

        vertx.eventBus().consumer(this.xpnsServicesEventAddress, this::xpnsServiceEventHandler);

        startDiscoverServices();

        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void startDiscoverServices() {
        vertx.executeBlocking(this::discoverServices, this::disoverServicesHandler);
    }

    private void discoverServices(Future f) {
        try {
            this.zooKeeper = new ZooKeeper(this.zkServers, 5000, null);

            String servicesPath = Directories.XPNS_SERVER_ROOT + "/" + Directories.API_SERVICES;
            List<String> services = this.zooKeeper.getChildren(servicesPath, this);

            vertx.eventBus().send(this.xpnsServicesEventAddress,
                    JSON.toJSONString(services.toArray(new String[]{})));

            f.complete();
        } catch (Exception ex) {
            logger.error("discover services error", ex);
            f.fail(ex);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
            try {
                String servicesPath = Directories.XPNS_SERVER_ROOT + "/" + Directories.API_SERVICES;
                List<String> services = this.zooKeeper.getChildren(servicesPath, this);

                vertx.eventBus().send(this.xpnsServicesEventAddress,
                        JSON.toJSONString(services.toArray(new String[]{})));
            } catch (Exception ex) {

                vertx.setTimer(1000, new Handler<Long>() {
                    @Override
                    public void handle(Long event) {
                        process(new WatchedEvent(Event.EventType.NodeChildrenChanged, null, null));
                    }
                });
            }
        }
    }

    private void disoverServicesHandler(AsyncResult result) {
        if (result.succeeded()) {
            this.startConsumer();
        }
    }

    private void xpnsServiceEventHandler(Message<String> msg) {
        String[] servicesArray = JSON.parseObject(msg.body(), this.stringArrayClazz);
        ArrayList<String> newServices = new ArrayList<>(servicesArray.length);
        for (String s :
                servicesArray) {
            newServices.add(s);
        }
        this.services = newServices;
        if (logger.isInfoEnabled()) {
            logger.info(String.format("xpns services %s", msg.body()));
        }
    }

    private void startConsumer() {
        this.kafkaConsumer = KafkaConsumer.create(vertx, config);
        this.kafkaConsumer.handler(this::kafkaConsumerHandler);
        this.kafkaConsumer.subscribe(this.topics, result -> {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("start kafka consumer %s", Boolean.toString(result.succeeded())));
            }
        });
    }

    private void kafkaConsumerHandler(KafkaConsumerRecord<String, String> record) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("got message key %s, value %s, partition %d, offset %d", record.key(),
                    record.value(),
                    record.partition(),
                    record.offset()));
        }

        NotificationEntity notificationEntity = null;
        try {
            notificationEntity = JSON.parseObject(record.value(), NotificationEntity.class);
        } catch (Exception ex) {
            logger.warn(String.format("decode notification entity error %s ", record.value()), ex);
        }


        if (notificationEntity == null || notificationEntity.getNotification() == null) {
            kafkaConsumer.commit(generateCommitOffset(record));
            return;
        }

        if (this.services == null || this.services.size() == 0) {
            offlineNotificationEntity(notificationEntity, record);
            return;
        }

        NotificationEntity entityEs = notificationEntity.clone();
        entityEs.setStatus(NotificationEntity.DEQUEUE);
        entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

        vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));

        this.sendNotification(notificationEntity, record);

    }

    private void offlineNotificationEntity(NotificationEntity entity,
                                           KafkaConsumerRecord<String, String> record) {
        //save to hbase

        //log to es
        NotificationEntity entityEs = entity.clone();
        entityEs.setStatus(NotificationEntity.OFFLINE);
        entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));
        vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));

        kafkaConsumer.commit(generateCommitOffset(record));
    }

    private void sendNotification(final NotificationEntity entity,
                                  final KafkaConsumerRecord<String, String> record) {

        try {
            JSONObject requestObject = new JSONObject();
            requestObject.put("acid", entity.getNotification().getTo());
            byte[] requestData = requestObject.toJSONString().getBytes("UTF-8");

            String service = this.services.get(rand.nextInt(this.services.size()));

            String[] hostPort = service.trim().split(":");
            httpClient.post(Integer.parseInt(hostPort[1]), hostPort[0], "/client")
                    .putHeader("Content-Type", "application/json;charset=UTF-8")
                    .putHeader("Content-Length", String.valueOf(requestData.length))
                    .handler(response -> {
                        response.bodyHandler(responseBuffer -> {
                           String s = new String(responseBuffer.getBytes());
                           JSONObject responseObject = JSONObject.parseObject(s);
                           if ("ok".equals(responseObject.getString("result"))) {
                               JSONObject sessionInfo = responseObject.getJSONObject("sessionInfo");
                               this.doRealSend(sessionInfo, entity, record);
                           } else {
                               offlineNotificationEntity(entity, record);
                           }
                        });
                    })
                    .write(Buffer.buffer(requestData)).end();

        } catch (Exception ex) {
            String message = String.format("send notification error %s",
                    entity.getNotification().toJSONObject().toJSONString());
            logger.warn(message, ex);
        }
    }

    private void doRealSend(final JSONObject sessionInfo,
                            final NotificationEntity entity,
                            final KafkaConsumerRecord<String, String> record) {

        String server = sessionInfo.getString("server");
        int port = sessionInfo.getInteger("port");


        try {

            byte[] requestData = entity.getNotification().toJSONObject().toJSONString()
                    .getBytes("UTF-8");

            HttpClient httpClient = vertx.createHttpClient();
            httpClient.post(port, server, "/notification")
                    .putHeader("Content-Type", "application/json;charset=UTF-8")
                    .putHeader("Content-Length", String.valueOf(requestData.length))
                    .exceptionHandler(t -> {
                        logger.warn(t);
                    })
                    .handler(response -> response.bodyHandler(responseBody -> {
                        String s = new String(responseBody.getBytes());
                        if (logger.isInfoEnabled()) {
                            logger.info(s);
                        }

                        kafkaConsumer.commit(generateCommitOffset(record));

                        NotificationEntity entityEs = entity.clone();
                        entityEs.setStatus(NotificationEntity.SENT);
                        entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

                        vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));
                    }))
                    .write(Buffer.buffer(requestData)).end();
        } catch (Exception ex) {
            String message = String.format("send notification error %s",
                    entity.getNotification().toJSONObject().toJSONString());
            logger.warn(message, ex);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> generateCommitOffset(KafkaConsumerRecord<String, String> record) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(1);

        TopicPartition topicPartition = new TopicPartition();
        topicPartition.setPartition(record.partition());
        topicPartition.setTopic(record.topic());

        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata();
        offsetAndMetadata.setMetadata("");
        offsetAndMetadata.setOffset(record.offset());

        offsets.put(topicPartition, offsetAndMetadata);

        return offsets;
    }

    private Map<String, String> config;
    private Set<String> topics;
    private String zkServers;
    private ZooKeeper zooKeeper;

    private KafkaConsumer<String, String> kafkaConsumer;

    private HttpClient httpClient = vertx.createHttpClient();

    private String xpnsServicesEventAddress;
    private List<String> services;
    private Random rand;

    private Class<? extends String[]> stringArrayClazz;

    private static final Logger logger = LoggerFactory.getLogger(KafkaNotificationVerticle.class);
}
