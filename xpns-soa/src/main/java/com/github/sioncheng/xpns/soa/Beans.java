package com.github.sioncheng.xpns.soa;


import com.github.sioncheng.xpns.common.config.AppProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

@Component
public class Beans {

    @Bean
    public KafkaProducer<String, String> createKafkaProducer() {

        Map<String, String> config = AppProperties.getPropertiesWithPrefix("kafka.producer.");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(config);

        return kafkaProducer;
    }
}
