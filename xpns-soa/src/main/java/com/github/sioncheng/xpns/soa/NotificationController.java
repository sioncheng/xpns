package com.github.sioncheng.xpns.soa;

import com.alibaba.fastjson.JSON;
import com.github.sioncheng.xpns.common.client.Notification;
import com.github.sioncheng.xpns.common.config.AppProperties;
import com.github.sioncheng.xpns.common.date.DateFormatPatterns;
import com.github.sioncheng.xpns.common.storage.NotificationEntity;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
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

import java.util.Date;
import java.util.UUID;

@Controller
public class NotificationController {

    @Autowired
    private KafkaProducer<String, String> kafkaProducer;

    @RequestMapping(value = "/notification",
            method = RequestMethod.POST,
            produces = "application/json;charset=UTF-8")
    @ResponseBody  public DeferredResult<NotificationResult> notificationHandler(
            @RequestBody final NotificationRequest notificationRequest) {
        DeferredResult<NotificationResult> result = new DeferredResult<>();
        if (notificationRequest == null) {
            NotificationResult notificationResult = new NotificationResult();
            notificationResult.setResult("error");

            result.setResult(notificationResult);
        } else {

            if (StringUtils.isEmpty(notificationRequest.getUniqId())) {
                notificationRequest.setUniqId((UUID.randomUUID().toString()));
            }

            NotificationEntity notificationEntity = new NotificationEntity();
            notificationEntity.setCreateDateTime(DateFormatUtils.format(new Date(),
                    DateFormatPatterns.ISO8601_WITH_MS));
            notificationEntity.setTtl(3600);
            notificationEntity.setStatus(NotificationEntity.NEW);
            Notification notification1 = new Notification();
            notification1.setUniqId(notificationRequest.getUniqId());
            notification1.setTo(notificationRequest.getTo());
            notification1.setTitle(notificationRequest.getTitle());
            notification1.setBody(notificationRequest.getBody());
            notification1.setExt(notificationRequest.getExt());
            notificationEntity.setNotification(notification1);
            notificationEntity.setTtl(notificationRequest.getTtl());

            String topic = AppProperties.getString("kafka-notification-topic");
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                    notificationRequest.getTo(),
                    JSON.toJSONString(notificationEntity));

            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    NotificationResult notificationResult = new NotificationResult();
                    notificationResult.setMessageId(notificationRequest.getUniqId());
                    if (e != null) {
                        notificationResult.setResult("error");
                    } else {
                        notificationResult.setResult("ok");



                        String topicEs =AppProperties.getString("kafka-es");
                        ProducerRecord<String, String> recordEs = new ProducerRecord<>(topicEs,
                                notificationRequest.getTo(),
                                JSON.toJSONString(notificationEntity));

                        kafkaProducer.send(recordEs);
                    }

                    result.setResult(notificationResult);
                }
            });
        }

        return result;
    }
}
