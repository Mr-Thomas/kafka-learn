package com.wwj.kafkalearn.message;

import com.wwj.kafkalearn.utils.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class MsgProducer {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    public Map<String, Object> sendMessageToKafkaServer(String jsonMessage, String topicName, boolean isASyncSend, String key) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("topicName", topicName);
        try {
            ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send(topicName, key, jsonMessage);
            if (!isASyncSend) {
                SendResult<String, Object> object = listenableFuture.get();
                Object value = object.getProducerRecord().value();
                long offset = object.getRecordMetadata().offset();
                int partition = object.getRecordMetadata().partition();
                log.info("sendMessageToKafkaServer topicName={},offset={}, partition={}, msg={}", topicName, offset, partition, value);
                result.put("code", 200);
                result.put("partition", partition);
                result.put("offset", offset);
                result.put("msg", value);
            } else {
                listenableFuture.addCallback(o ->
                                log.info("send message to kafka success !!! topicName={}, offset={},msg={}", topicName,
                                        o.getRecordMetadata().offset(), o.getProducerRecord().value())
                        , throwable ->
                                this.sendMsgFail(throwable, topicName));
            }
        } catch (Exception e) {
            log.error("send message to kafka error topicName={}", topicName, e);
            result.put("code", 500);
            result.put("msg", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> sendMessageToKafkaServer(Object jsonMessage, String topicName, boolean isASyncSend, String key) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("topicName", topicName);
        try {
            ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send(topicName, key, jsonMessage);
            if (!isASyncSend) {
                SendResult<String, Object> object = listenableFuture.get();
                Object value = object.getProducerRecord().value();
                long offset = object.getRecordMetadata().offset();
                int partition = object.getRecordMetadata().partition();
                log.info("sendMessageToEgl topicName={},offset={}, partition={}, msg={}", topicName, offset, partition, value);
                result.put("code", 200);
                result.put("partition", partition);
                result.put("offset", offset);
                result.put("msg", value);
            } else {
                listenableFuture
                        .addCallback(
                                o -> log.warn("send message to kafka success !!! topicName={}, offset={},msg={}", topicName,
                                        o.getRecordMetadata().offset(), o.getProducerRecord().value()),
                                throwable -> this.sendMsgFail(throwable, topicName));
            }
        } catch (Exception e) {
            log.error("send message to kafka error topicName={}", topicName, e);
            result.put("code", 500);
            result.put("msg", e.getMessage());
        }
        return result;
    }

    public Map<String, Object> sendMessageToKafkaServer(Object jsonMessage, String topicName, boolean isASyncSend) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("topicName", topicName);
        try {
            Map<String, Object> headerParam = new HashMap<>();
            headerParam.put(KafkaHeaders.TOPIC, topicName);
            Message<?> message = new GenericMessage<>(jsonMessage, headerParam);
            ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send(message);
            if (!isASyncSend) {
                SendResult<String, Object> object = listenableFuture.get();
                Object value = object.getProducerRecord().value();
                long offset = object.getRecordMetadata().offset();
                int partition = object.getRecordMetadata().partition();
                log.info("sendMessageToKafkaServer topicName={},offset={}, partition={}, msg={}", topicName, offset, partition, value);
                result.put("code", 200);
                result.put("partition", partition);
                result.put("offset", offset);
                result.put("msg", value);
            } else {
                listenableFuture.addCallback(o ->
                                log.info("send message to kafka success !!! topicName={}, offset={},msg={}", topicName,
                                        o.getRecordMetadata().offset(), o.getProducerRecord().value())
                        , throwable ->
                                this.sendMsgFail(throwable, topicName));
            }
        } catch (Exception e) {
            log.error("send message to kafka error topicName={}", topicName, e);
            result.put("code", 500);
            result.put("msg", e.getMessage());
        }
        return result;
    }

    public void sendMessage(Object messageObject, String topicName) {
        try {
            log.info("send message to kafka topicName= " + topicName + "," + JsonUtil.object2jackJson(messageObject));
            Map<String, Object> headerParam = new HashMap<>();
            headerParam.put(KafkaHeaders.TOPIC, topicName);
            Message<?> kafkaMsg = new GenericMessage<>(messageObject, headerParam);
            ListenableFuture<SendResult<String, Object>> listenableFuture = kafkaTemplate.send(kafkaMsg);
            listenableFuture.addCallback(
                    o -> log.info("send message to kafka success !!! topicName={}, offset={},msg={}", topicName,
                            o.getRecordMetadata().offset(), o.getProducerRecord().value()),
                    throwable -> this.sendMsgFail(throwable, topicName));
        } catch (Exception e) {
            log.error("send message to kafka error topicName= " + topicName, e);
            e.printStackTrace();
        }
    }

    private void sendMsgFail(Throwable throwable, String topicName) {
        log.error("send message to kafka fail !!! topicName=" + topicName + " error " + throwable.getMessage());
    }

}
