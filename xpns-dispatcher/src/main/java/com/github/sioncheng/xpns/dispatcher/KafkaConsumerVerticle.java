package com.github.sioncheng.xpns.dispatcher;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class KafkaConsumerVerticle extends AbstractVerticle {

    public KafkaConsumerVerticle(Map<String, String> config, Set<String> topics) {
        this.config = config;
        this.topics = topics;
    }

    @Override
    public void start() throws Exception {

        startConsumer();

        super.start();
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    private void startConsumer() {
        this.kafkaConsumer = KafkaConsumer.create(vertx, config);
        this.kafkaConsumer.handler(this::consumerHandler);
        this.kafkaConsumer.subscribe(this.topics, result -> {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("start kafka consumer %s", Boolean.toString(result.succeeded())));
            }
        });
    }

    private void consumerHandler(KafkaConsumerRecord<String, String> record) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("got message key %s, value %s, partition %d, offset %d", record.key(),
                    record.value(),
                    record.partition(),
                    record.offset()));
        }
    }

    private Map<String, String> config;
    private Set<String> topics;

    private KafkaConsumer<String, String> kafkaConsumer;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerVerticle.class);
}
