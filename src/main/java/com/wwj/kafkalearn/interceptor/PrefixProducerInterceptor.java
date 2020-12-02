package com.wwj.kafkalearn.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.text.DecimalFormat;
import java.util.Map;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/11/3 000315:44
 * 自定义消息发送拦截器,消息前加个前缀test
 */
@Slf4j
public class PrefixProducerInterceptor implements org.apache.kafka.clients.producer.ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFail = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        String value = "test_" + producerRecord.value();
        ProducerRecord<String, String> record = new ProducerRecord<>(producerRecord.topic(), producerRecord.key(), value);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (null == e) {
            sendSuccess++;
        } else {
            sendFail++;
        }
    }

    @Override
    public void close() {
        log.info("sendSuccess:{}",sendSuccess);
        log.info("sendFail:{}",sendFail);
        String format = new DecimalFormat("0.00%").format(sendSuccess / (sendSuccess + sendFail));
        log.info("消息发送成功率：{}", format);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
