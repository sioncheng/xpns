package com.github.sioncheng.xpns.dispatcher;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.client.SessionInfo;
import com.github.sioncheng.xpns.common.config.AppProperties;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

        this.httpClient = vertx.createHttpClient();

        if ("hbase".equalsIgnoreCase(AppProperties.getString("store.type"))) {
            initHbase();
        }
        if ("mysql".equalsIgnoreCase(AppProperties.getString("store.type"))) {
            initMariaDb();
        }

        vertx.eventBus().consumer(this.xpnsServicesEventAddress, this::xpnsServiceEventHandler);

        startDiscoverServices();

        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void initHbase() throws Exception {
        Map<String, String> hbaseConf = AppProperties.getPropertiesStartwith("hbase");
        Configuration configuration = HBaseConfiguration.create();
        for (Map.Entry<String, String> hconf :
                hbaseConf.entrySet()) {
            configuration.set(hconf.getKey(), hconf.getValue());
        }
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    private void initMariaDb() {
        Map<String, String> appConf = AppProperties.getPropertiesWithPrefix("mysql.");

        JsonObject confJson = new JsonObject();
        for (Map.Entry<String, String> conf:
             appConf.entrySet()) {
            if (StringUtils.isNumeric(conf.getValue())) {
                confJson.put(conf.getKey(), Integer.parseInt(conf.getValue()));
            } else {
                confJson.put(conf.getKey(), conf.getValue());
            }
        }

        this.asyncSQLClient = MySQLClient.createShared(vertx, confJson);
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
                logger.info(String.format("start notification kafka consumer %s", Boolean.toString(result.succeeded())));
            }
        });
    }

    private void kafkaConsumerHandler(KafkaConsumerRecord<String, String> record) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("got message key %s, value %s, partition %d, offset %d, topic %s", record.key(),
                    record.value(),
                    record.partition(),
                    record.offset(),
                    record.topic()));
        }

        if (AppProperties.getString("kafka-logon-topic").equals(record.topic())) {
            kafkaConsumer.commit(generateCommitOffset(record));
            this.kafkaConsumerLogonHandler(record);
        } else {
            this.kafkaConsumerNotificationHandler(record);
        }
    }

    private void kafkaConsumerNotificationHandler(KafkaConsumerRecord<String, String> record) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("notification handler %s", record.value()));
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
            logger.warn("no xpns services");
            offlineNotificationEntity(notificationEntity, record);
            return;
        }

        if (notificationEntity.getStatus() == NotificationEntity.NEW) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("dispatch new notification %s",
                        notificationEntity.getNotification().toJSONObject().toString()));
            }
            NotificationEntity entityEs = notificationEntity.clone();
            entityEs.setStatus(NotificationEntity.DEQUEUE);
            entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

            vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));

            this.sendNotification(notificationEntity, record);
        } else {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("dispatch notification %s status %d",
                        notificationEntity.getNotification().getUniqId(),
                        notificationEntity.getStatus()));
            }
            NotificationEntity entityEs = notificationEntity.clone();
            entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

            vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));

            if (notificationEntity.getStatus() == NotificationEntity.DELIVERED) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("save delivered notification %s ",
                            notificationEntity.getNotification().getUniqId()));
                }

                Exception exception = storeNotification(notificationEntity.clone());
                if (exception != null) {
                    logger.warn(String.format("store notification error %s", exception.getMessage()));
                }
            }

            kafkaConsumer.commit(generateCommitOffset(record));
        }

    }

    private void offlineNotificationEntity(NotificationEntity entity,
                                           KafkaConsumerRecord<String, String> record) {
        //save to hbase
        vertx.executeBlocking(future -> {
            NotificationEntity notificationHbase = entity.clone();
            notificationHbase.setStatus(NotificationEntity.OFFLINE);
            notificationHbase.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

            Exception exception = storeNotification(notificationHbase);
            if (exception == null) {
                future.complete();
            } else {
                future.fail(exception);
            }
        },
        false,
        result -> {
            //log to es
            NotificationEntity entityEs = entity.clone();
            entityEs.setStatus(NotificationEntity.OFFLINE);
            entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));
            vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));
            if (result.succeeded()) {
                kafkaConsumer.commit(generateCommitOffset(record));
            }
        });


    }

    private Exception storeNotification(NotificationEntity entity)  {
        if ("hbase".equalsIgnoreCase(AppProperties.getString("store.type"))) {
            return putToHBase(entity);
        } else {
            return putToMariaDb(entity);
        }
    }

    private Exception putToHBase(NotificationEntity entity) {
        Exception exception = null;
        Table table = null;
        try {
            Notification notification = entity.getNotification();

            String rowKey = StringUtils.reverse(notification.getTo()) +
                    notification.getUniqId();
            Put put = new Put(rowKey.getBytes());

            byte[] family = "cf".getBytes();

            put.addColumn(family, "to".getBytes(), notification.getTo().getBytes());
            put.addColumn(family, "ttl".getBytes(), String.valueOf(entity.getTtl()).getBytes());
            if (StringUtils.isNotEmpty(entity.getCreateDateTime())) {
                put.addColumn(family, "createDateTime".getBytes(), entity.getCreateDateTime().getBytes());
            }
            put.addColumn(family, "status".getBytes(), String.valueOf(entity.getStatus()).getBytes());
            put.addColumn(family, "statusDateTime".getBytes(), entity.getStatusDateTime().getBytes());
            put.addColumn(family, "notification".getBytes(), notification.toJSONObject().toJSONString().getBytes("UTF-8"));

            table = connection.getTable(TableName.valueOf("notification"));
            table.put(put);

        } catch (Exception ex) {
            logger.warn("do real save error", ex);
            exception = ex;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (Exception ex) {}
            }
        }

        return exception;
    }

    private Exception putToMariaDb(NotificationEntity entity) {

        if (NotificationEntity.DELIVERED == entity.getStatus() &&
                StringUtils.isEmpty(entity.getCreateDateTime())) {
            refillAndPutToDb(entity);
            return null;
        }

        Exception exception = null;

        try {
            //acid, create_date_time, ttl, ttl_date_time , status, status_date_time
            //notification

            Date createDateTime = DateUtils.parseDate(entity.getCreateDateTime(), DateFormatPatterns.ISO8601_WITH_MS);
            Date statusDateTime = DateUtils.parseDate(entity.getStatusDateTime(), DateFormatPatterns.ISO8601_WITH_MS);

            JsonArray parameters = new JsonArray();
            parameters.add(entity.getNotification().getTo());
            parameters.add(entity.getCreateDateTime());
            parameters.add(entity.getTtl());
            Date ttlDateTime = new Date(createDateTime.getTime() + entity.getTtl() * 1000);
            parameters.add(DateFormatUtils.format(ttlDateTime, DateFormatPatterns.NORMAL_T_WITH_MS));
            parameters.add(entity.getStatus());
            parameters.add(DateFormatUtils.format(statusDateTime, DateFormatPatterns.NORMAL_T_WITH_MS));
            parameters.add(JSONObject.toJSONString(entity.getNotification()));
            parameters.add(entity.getNotification().getUniqId());
            asyncSQLClient.updateWithParams("insert into notification(acid, create_date_time, " +
                            "ttl, ttl_date_time, status, status_date_time, notification, uuid) " +
                            "values (?,?,?,?,?,?,?,?) ",
                    parameters,
                    asyncResult -> {
                        if (asyncResult.failed()) {
                            logger.warn("insert into notification error " + asyncResult.cause().getMessage());
                        }
                    });

        } catch (Exception ex) {
            exception = ex;
        }
        return exception;
    }

    private Exception refillAndPutToDb(NotificationEntity entity) {
        JsonArray jsonArray = new JsonArray();
        jsonArray.add(entity.getNotification().getTo());
        jsonArray.add(entity.getNotification().getUniqId());

        asyncSQLClient.queryWithParams("select acid, create_date_time, " +
                        "ttl, ttl_date_time, status, status_date_time, notification, uuid " +
                        "from notification where acid = ? and uuid = ? ",
                jsonArray,
                asyncResult -> {
                    if (asyncResult.succeeded()) {
                        List<JsonObject> rows = asyncResult.result().getRows();
                        if (rows.size() == 0) {
                            logger.warn("cant find previous entity");
                        } else {
                            JsonObject row = rows.get(0);
                            NotificationEntity deliveredEntity = new NotificationEntity();

                            try {
                                Date createDateTime = DateUtils.parseDate(row.getString("create_date_time"),
                                        DateFormatPatterns.NORMAL_T_WITH_MS);

                                deliveredEntity.setCreateDateTime(DateFormatUtils.format(createDateTime,
                                        DateFormatPatterns.ISO8601_WITH_MS));
                                deliveredEntity.setNotification(JSONObject.parseObject(row.getString("notification"),
                                        Notification.class));
                                deliveredEntity.setStatus(NotificationEntity.DELIVERED);
                                deliveredEntity.setStatusDateTime(DateFormatUtils.format(new Date(),
                                        DateFormatPatterns.ISO8601_WITH_MS));
                                deliveredEntity.setTtl(row.getInteger("ttl"));
                                Exception ex = putToMariaDb(deliveredEntity);
                                if (ex != null) {
                                    logger.warn("refillAndPutToDb error " + ex.getMessage());
                                }
                            } catch (Exception ex) {
                                logger.warn("refillAndPutToDb error " + ex.getMessage());
                            }
                        }
                    } else {
                        logger.warn("refillAndPutToDb error " + asyncResult.cause().getMessage());
                    }
                } );

        return null;
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
                    .exceptionHandler(t -> {
                        logger.warn(String.format("connect to %s error", service), t);
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
                        logger.warn(String.format("doRealSend error %s", t.getMessage()));

                        offlineNotificationEntity(entity, record);
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

                        storeNotification(entityEs);
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

    private void kafkaConsumerLogonHandler(KafkaConsumerRecord<String, String> record) {
        SessionInfo sessionInfo = JSON.parseObject(record.value(), SessionInfo.class);

        if (sessionInfo == null) {
            logger.warn(String.format("unable to parse session info %s", record.value()));
            return;
        }

        vertx.executeBlocking(future -> {
            Exception ex = mayResendNotification(sessionInfo);
            if (ex == null) {
                future.complete();
            } else {
                future.fail(ex);
            }
        },
        true,
        result -> {
            if (result.succeeded()) {
                if (logger.isInfoEnabled()) {
                    logger.info("may resend notification success");
                }
            } else {
                logger.warn("may resend notification error", result.cause());
            }
        });
    }

    private Exception mayResendNotification(SessionInfo sessionInfo) {
        if ("hbase".equalsIgnoreCase(AppProperties.getString("store.type"))) {
            return mayResendNotificationFromHBase(sessionInfo);
        } else {
            return mayResendNotificationFromMariaDb(sessionInfo);
        }
    }

    private Exception mayResendNotificationFromHBase(SessionInfo sessionInfo) {
        Exception exception = null;
        Table table = null;
        ResultScanner scanner = null;
        try {
            Scan scan = new Scan();
            scan.setRowPrefixFilter(StringUtils.reverse(sessionInfo.getAcid()).getBytes());
            SingleColumnValueFilter statusFilter = new SingleColumnValueFilter("cf".getBytes(),
                    "status".getBytes(),
                    CompareFilter.CompareOp.EQUAL,
                    String.valueOf(NotificationEntity.OFFLINE).getBytes());
            scan.setFilter(statusFilter);

            table = connection.getTable(TableName.valueOf("notification"));

            scanner = table.getScanner(scan);

            byte[] family = "cf".getBytes();
            byte[] toQualifier = "to".getBytes();
            byte[] createDateTimeQualifier = "createDateTime".getBytes();
            byte[] ttlQualifier = "ttl".getBytes();
            byte[] statusQualifier = "status".getBytes();
            byte[] statusDateTimeQualifier = "statusDateTime".getBytes();
            byte[] notificationQualifier = "notification".getBytes();

            Result result = scanner.next();
            while (result != null) {
                NotificationEntity offlineEntity = new NotificationEntity();
                String to = HBaseHelper.getValue(result, family, toQualifier);
                String createDateTime = HBaseHelper.getValue(result, family, createDateTimeQualifier);
                int ttl = Integer.parseInt(HBaseHelper.getValue(result, family, ttlQualifier));
                int status = Integer.parseInt(HBaseHelper.getValue(result, family, statusQualifier));
                String statusDateTime = HBaseHelper.getValue(result, family, statusDateTimeQualifier);
                String notification = HBaseHelper.getValue(result, family, notificationQualifier);

                offlineEntity.setStatus(status);
                offlineEntity.setStatusDateTime(statusDateTime);
                offlineEntity.setTtl(ttl);
                offlineEntity.setCreateDateTime(createDateTime);
                offlineEntity.setNotification(JSON.parseObject(notification, Notification.class));

                resendNotification(offlineEntity, sessionInfo);

                result = scanner.next();
            }
            scanner.close();
            table.close();
        } catch (Exception ex) {
            logger.warn("kafka consumer logon handler error", ex);

            if (scanner != null) {
                scanner.close();
            }
            if (table != null) {
                try {
                    table.close();
                } catch (Exception ex1) {}
            }

            exception = ex;
        }

        return exception;
    }

    private Exception mayResendNotificationFromMariaDb(final SessionInfo sessionInfo) {
        Exception exception = null;

        JsonArray parameters = new JsonArray();
        parameters.add(sessionInfo.getAcid());

        this.asyncSQLClient.queryWithParams ("select id, acid, create_date_time , ttl, ttl_date_time," +
                " status, status_date_time, notification, uuid from notification " +
                " where acid = ? and ttl_date_time > current_timestamp ",
                parameters,
                asyncResult -> {
                    if (asyncResult.succeeded()) {
                        List<JsonObject> rows = asyncResult.result().getRows();
                        if (logger.isInfoEnabled()) {
                            logger.info(String.format("got %d records for %s", rows.size(), sessionInfo.getAcid()));
                        }

                        HashMap<String, List<JsonObject>> resultGroup=new HashMap<>();
                        List<String> delivered = new ArrayList<>();
                        for (JsonObject row :
                                rows) {
                            String uuid = row.getString("uuid");
                            List<JsonObject> list;
                            if (resultGroup.containsKey(uuid)) {
                                list = resultGroup.get(uuid);
                            } else {
                                list = new ArrayList<>();
                                resultGroup.put(uuid, list);
                            }

                            list.add(row);
                            if (logger.isInfoEnabled()) {
                                logger.info(String.format("add %s for %s", uuid, sessionInfo.getAcid()));
                            }

                            if (NotificationEntity.DELIVERED == row.getInteger("status")) {
                                delivered.add(uuid);
                                if (logger.isInfoEnabled()) {
                                    logger.info(String.format("skip delivered records %s %s", uuid, sessionInfo.getAcid()));
                                }
                            }
                        }

                        for (String uuid :
                                delivered) {
                            resultGroup.remove(uuid);
                        }

                        if (logger.isInfoEnabled()) {
                            logger.info(String.format("resultGroup size %d for %s", resultGroup.size(), sessionInfo.getAcid()));
                        }

                        for (Map.Entry<String, List<JsonObject>> result :
                            resultGroup.entrySet()) {
                            try {
                                JsonObject row = result.getValue().get(0);

                                NotificationEntity offlineEntity = new NotificationEntity();

                                Date createDateTime = DateUtils.parseDate(row.getString("create_date_time"),
                                        DateFormatPatterns.NORMAL_T_WITH_MS);
                                Date statusDateTime = DateUtils.parseDate(row.getString("status_date_time"),
                                        DateFormatPatterns.NORMAL_T_WITH_MS);

                                offlineEntity.setCreateDateTime(DateFormatUtils.format(createDateTime,
                                        DateFormatPatterns.ISO8601_WITH_MS));
                                offlineEntity.setNotification(JSONObject.parseObject(row.getString("notification"),
                                        Notification.class));
                                offlineEntity.setStatus(row.getInteger("status"));
                                offlineEntity.setStatusDateTime(DateFormatUtils.format(statusDateTime,
                                        DateFormatPatterns.ISO8601_WITH_MS));
                                offlineEntity.setTtl(row.getInteger("ttl"));

                                if (logger.isInfoEnabled()) {
                                    logger.info(String.format("resend %s", row.getString("notification")));
                                }
                                resendNotification(offlineEntity, sessionInfo);
                            } catch (Exception ex) {
                                logger.warn("mayResendNotificationFromMariaDb error " + ex.getMessage());
                            }
                        }
                    } else {
                        logger.warn("mayResendNotificationFromMariaDb error " + asyncResult.cause().getMessage());
                    }
                }) ;

        return exception;
    }


    private void resendNotification(final NotificationEntity offlineEntity,
                                    SessionInfo sessionInfo) {
        try {

            byte[] requestData = offlineEntity.getNotification().toJSONObject().toJSONString()
                    .getBytes("UTF-8");

            HttpClient httpClient = vertx.createHttpClient();
            httpClient.post(sessionInfo.getPort(), sessionInfo.getServer(), "/notification")
                    .putHeader("Content-Type", "application/json;charset=UTF-8")
                    .putHeader("Content-Length", String.valueOf(requestData.length))
                    .exceptionHandler(t -> {
                        logger.warn("resendNotification error " + t.getMessage());
                    })
                    .handler(response -> response.bodyHandler(responseBody -> {
                        String s = new String(responseBody.getBytes());
                        if (logger.isInfoEnabled()) {
                            logger.info(s);
                        }

                        NotificationEntity entityEs = offlineEntity.clone();
                        entityEs.setStatus(NotificationEntity.RESEND);
                        entityEs.setStatusDateTime(DateFormatUtils.format(new Date(), DateFormatPatterns.ISO8601_WITH_MS));

                        vertx.eventBus().send(KafkaEsVerticle.NotificationEntityESEventAddress, JSON.toJSONString(entityEs));
                    }))
                    .write(Buffer.buffer(requestData)).end();
        } catch (Exception ex) {
            String message = String.format("send notification error %s",
                    offlineEntity.getNotification().toJSONObject().toJSONString());
            logger.warn(message, ex);
        }
    }

    private Map<String, String> config;
    private Set<String> topics;
    private String zkServers;
    private ZooKeeper zooKeeper;

    private KafkaConsumer<String, String> kafkaConsumer;

    private HttpClient httpClient;

    private Connection connection;

    private AsyncSQLClient asyncSQLClient;

    private String xpnsServicesEventAddress;
    private List<String> services;
    private Random rand;

    private Class<? extends String[]> stringArrayClazz;

    private static final Logger logger = LoggerFactory.getLogger(KafkaNotificationVerticle.class);
}
