package com.wwj.kafkalearn.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author ：Administrator
 * @description：TODO
 * @date ：2021/12/9 15:24
 */
@Component
@Slf4j
public class MsgConsumer {

    @KafkaListener(topics = "topic", groupId = "kafka-demo-topic-group")
    public void onMessage(ConsumerRecord<?, ?> record) {
        log.info("topic:{},partition:{},offset:{},value:{}", record.topic(), record.partition(), record.offset(), record.value());
    }
}
