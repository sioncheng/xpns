package com.github.sioncheng.xpns.dispatcher;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KafkaAckVerticle extends AbstractVerticle  {

    public KafkaAckVerticle(Map<String, String> config, Set<String> topics) {
        this.config = config;
        this.topics = topics;
    }

    @Override
    public void start() throws Exception {

        kafkaConsumer = KafkaConsumer.create(vertx, this.config);


        kafkaConsumer.handler(this::ackConsumerHandler);

        kafkaConsumer.subscribe(topics, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("start consume notification ack %s", event.succeeded()));
                }
            }
        });


        super.start();
    }

    @Override
    public void stop() throws Exception {


        super.stop();
    }

    private void ackConsumerHandler(KafkaConsumerRecord<String, String> record) {
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

    private KafkaConsumer<String, String> kafkaConsumer;

    private static final Logger logger = LoggerFactory.getLogger(KafkaAckVerticle.class);
}
