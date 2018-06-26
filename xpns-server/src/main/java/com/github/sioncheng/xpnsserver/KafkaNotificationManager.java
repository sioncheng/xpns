package com.github.sioncheng.xpnsserver;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.Notification;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class KafkaNotificationManager  {

    public KafkaNotificationManager(KafkaNotificationConsumerConfig consumerConfig) {

        this.consumerConfig = consumerConfig;
    }

    public void notificationAck(Notification notification, boolean success) {
        if (logger.isInfoEnabled()) {
            logger.info("notification ack {} {}", success, notification.toJSONObject().toJSONString());
        }
    }


    private void createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", this.consumerConfig.getBootstrapServer());
        props.put("group.id", this.consumerConfig.getGroupId());//消费者的组id
        props.put("enable.auto.commit", Boolean.toString(this.consumerConfig.isEnableAutoCommit()));
        props.put("auto.commit.interval.ms", Integer.valueOf(this.consumerConfig.getAutoCommitIntervalMS()));
        props.put("auto.offset.reset", this.consumerConfig.getAutoOffsetReset());
        props.put("session.timeout.ms", Integer.valueOf(this.consumerConfig.getSessionTimeoutMS()));
        props.put("key.deserializer", this.consumerConfig.getKeyDeserializer());
        props.put("value.deserializer", this.consumerConfig.getValueDeserializer());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("xpns-notification"));

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);

                    if (records == null || records.count() == 0) {
                        try {
                            Thread.sleep(10);
                        } catch (Exception ex) {

                        }

                        continue;
                    }

                    for (ConsumerRecord<String, String> record :
                            records) {

                        Notification notification = JSON.parseObject(record.value(), Notification.class);

                    }

                    consumer.commitSync();
                }
            }
        });
        t.setDaemon(true);
        t.setName("xpns-notification-consumer");

        t.start();


    }

    KafkaNotificationConsumerConfig consumerConfig;

    private KafkaConsumer<String, String> consumer;

    private Logger logger = LoggerFactory.getLogger(KafkaNotificationManager.class);
}
