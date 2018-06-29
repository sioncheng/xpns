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
        Properties properties = new Properties();
        for (Map.Entry<String, String> kv :
                config.entrySet()) {
            properties.put(kv.getKey(), kv.getValue());
        }


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);

        return kafkaProducer;
    }
}
