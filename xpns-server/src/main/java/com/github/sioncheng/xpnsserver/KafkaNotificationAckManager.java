package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaNotificationAckManager {

    public KafkaNotificationAckManager(Properties producerConfig, String ackTopic) {
        this.kafkaProducer = new KafkaProducer<>(producerConfig);
        this.ackTopic = ackTopic;
    }

    public void notificationAck(Notification notification, boolean success) {
        if (logger.isInfoEnabled()) {
            logger.info("notification ack {} {}", success, notification.toJSONObject().toJSONString());
        }

        NotificationEntity notificationEntity = new NotificationEntity();
        notificationEntity.setNotification(notification);
        if (success) {
            notificationEntity.setStatus(NotificationEntity.DELIVERED);
        } else {
            notificationEntity.setStatus(NotificationEntity.UNDELIVER);
        }

        final String value = JSON.toJSONString(notificationEntity);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(notification.getTo(), value);

        this.kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    logger.warn(String.format("send notification ack error %s", value), e);
                }
            }
        });
    }


    private KafkaProducer<String, String> kafkaProducer;

    private String ackTopic;

    private Logger logger = LoggerFactory.getLogger(KafkaNotificationAckManager.class);
}
