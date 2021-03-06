package com.github.sioncheng.xpns.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.sioncheng.xpns.common.config.AppProperties;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.vertx.core.impl.VertxImpl.context;

public class ElasticsearchVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {

        this.startConsumer();

        this.httpClient = vertx.createHttpClient();

        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void startConsumer() {
        Map<String, String> consumerConfig = AppProperties.getPropertiesWithPrefix("kafka.consumer.");

        kafkaConsumer = KafkaConsumer.create(vertx, consumerConfig);
        kafkaConsumer.handler(this::kafkaConsumerHandler);
        kafkaConsumer.subscribe(AppProperties.getString("kafka-es-topic"), result -> {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("start consumer for es %s", result.succeeded()));
            }
        });

    }

    private void kafkaConsumerHandler(KafkaConsumerRecord<String, String> record) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("got message %s", record.value()));
        }

        NotificationEntity entity = JSON.parseObject(record.value(), NotificationEntity.class);

        this.index(entity, record);
    }

    private void index(NotificationEntity entity,
                       KafkaConsumerRecord<String, String> record) {
        /*
        * POST /notification/trace
            {
              "acid":"32000000000000000001",
              "to":"42000000000000000001",
              "title":"hello",
              "body":"where are you",
              "ext":"{}",
              "uniqId":"ece4f4a60b764339b94a07c84e338a27",
              "createDateTime":"2018-12-12T13:55:00+08:00",
              "ttl":3600,
              "status":1,
              "statusDateTime":"2018-12-12T13:55:00+08:00"
            }
        * */

        final String path = "/notification/trace";
        JSONObject doc = new JSONObject();
        doc.put("acid", entity.getNotification().getTo());
        doc.put("to", entity.getNotification().getTo());
        doc.put("title", entity.getNotification().getTitle());
        doc.put("body", entity.getNotification().getBody());
        doc.put("ext", entity.getNotification().getExt().toJSONString());
        doc.put("uniqId", entity.getNotification().getUniqId());
        doc.put("createDateTime", entity.getCreateDateTime());
        doc.put("ttl", entity.getTtl());
        doc.put("status", entity.getStatus());
        doc.put("statusDateTime", entity.getStatusDateTime());



        byte[] postData = null;
        try {
            postData = doc.toJSONString().getBytes("UTF-8");
        } catch (Exception ex) {
            logger.error("index error", ex);
            return;
        }
        final HttpClientRequest request = httpClient.postAbs(AppProperties.getString("elasticsearch.url") +  path);
        request.exceptionHandler(t -> {
            logger.warn("post entity to es index error", t);
        });
        request.handler(response -> {
            response.exceptionHandler(ex->{
                logger.error("response error", ex);
            });
            response.endHandler((Void)->{
                logger.info("response end");
            });
            response.bodyHandler(responseBuffer -> {
                String responseBody = new String(responseBuffer.getBytes());
                if (logger.isInfoEnabled()) {
                    logger.info(responseBody);
                }

                JSONObject esResponseObject = JSON.parseObject(responseBody);
                if ("created".equals(esResponseObject.getString("result"))) {
                    commit(record);
                }
            });
        });
        request.putHeader("Content-Type", "application/json;charset=UTF-8")
                .putHeader("Content-Length", String.valueOf(postData.length))
                .end(Buffer.buffer(postData));


    }

    private void commit(KafkaConsumerRecord<String, String> record) {

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(1);

        TopicPartition topicPartition = new TopicPartition();
        topicPartition.setPartition(record.partition());
        topicPartition.setTopic(record.topic());

        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata();
        offsetAndMetadata.setMetadata("");
        offsetAndMetadata.setOffset(record.offset());

        offsets.put(topicPartition, offsetAndMetadata);

        kafkaConsumer.commit(offsets);

    }


    private KafkaConsumer<String, String> kafkaConsumer;
    private HttpClient httpClient;
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchVerticle.class);
}
