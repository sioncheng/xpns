package com.github.sioncheng.xpns.dispatcher;

import com.github.sioncheng.xpns.common.config.AppProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class KafkaEsVerticle extends AbstractVerticle {

    public static final String NotificationEntityESEventAddress = "notification.es";

    public KafkaEsVerticle() {

    }

    @Override
    public void start() throws Exception {

        this.createProducer();

        vertx.eventBus().consumer(NotificationEntityESEventAddress, this::notificationEntityToEsHandler);

        super.start();
    }

    @Override
    public void stop() throws Exception {


        super.stop();
    }

    private void createProducer() {
        int instances = AppProperties.getInt("server.instances");
        Map<String, String> producerConfig = AppProperties.getPropertiesWithPrefix("kafka.producer.");

        kafkaProducerList = new ArrayList<>(instances);
        for(int i = 0 ; i < instances; i++) {
            KafkaProducer<String, String> kafkaProducer =
                    KafkaProducer.create(vertx, producerConfig);
            kafkaProducerList.add(kafkaProducer);
        }
    }

    private void notificationEntityToEsHandler(Message<String> message) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("got event %s", message.body()));
        }

        KafkaProducerRecord<String, String> record =
                new KafkaProducerRecordImpl<>(AppProperties.getString("kafka-es"), message.body());
        this.kafkaProducerList.get(rand.nextInt(this.kafkaProducerList.size())).write(record);
    }


    private List<KafkaProducer<String, String>> kafkaProducerList;

    private static final Logger logger = LoggerFactory.getLogger(KafkaEsVerticle.class);
    private static final Random rand = new Random();
}
