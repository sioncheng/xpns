package com.github.sioncheng.xpns.soa;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.config.AppProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.UUID;

@Controller
public class NotificationController {

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    @RequestMapping(value = "/notification",
            method = RequestMethod.POST,
            produces = "application/json;charset=UTF-8")
    @ResponseBody  public DeferredResult<NotificationResult> notificationHandler(
            @RequestBody final Notification notification) {
        DeferredResult<NotificationResult> result = new DeferredResult<>();
        if (notification == null) {
            NotificationResult notificationResult = new NotificationResult();
            notificationResult.setResult("error");

            result.setResult(notificationResult);
        } else {

            if (StringUtils.isEmpty(notification.getUniqId())) {
                notification.setUniqId((UUID.randomUUID().toString()));
            }

            String topic = AppProperties.getString("kafka-notification-topic");
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    notification.getTo(),
                    JSON.toJSONString(notification));

            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    NotificationResult notificationResult = new NotificationResult();
                    notificationResult.setMessageId(notification.getUniqId());
                    if (e != null) {
                        notificationResult.setResult("error");
                    } else {
                        notificationResult.setResult("ok");
                    }

                    result.setResult(notificationResult);
                }
            });
        }

        return result;
    }
}
