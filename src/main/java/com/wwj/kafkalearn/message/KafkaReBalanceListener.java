package com.wwj.kafkalearn.message;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.security.PublicKey;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/11/10 001011:35
 * 再均衡监听器:
 * 再均衡是指分区的所属从一个消费者转移到另一个消费者的行为，它为消费组具备了高可用性和伸缩性提供了保障，
 * 使得我们既方便又安全地删除消费组内的消费者或者往消费组内添加消费者。再均衡发生期间，消费者时无法拉取消息的。
 */
@Slf4j
@Service
public class KafkaReBalanceListener {

    public final AtomicBoolean isRunning = new AtomicBoolean(true);

    public void reBalanceListenerConsumer(String topic, String groupId) {
        groupId = groupId == null ? "test" : groupId;
        Map<String, Object> configs = new HashMap<String, Object>();
        //设置key的序列化器 反序列化 和生产者相对应
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //设置值序列化器
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //设置集群地址
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //消费组，默认为空，如果设置为空则会抛出异常【写入一次消息，可支持任意多应用读取这个消息，使得每个应用都能读取到全量消息，应用需要有不同的消费组】
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //默认true，自动位移提交:消费者会在poll方法调用后每隔5秒【auto.commit.interval.ms指定】提交一次位移。可能会重复消费
        //false，手动提交位移【同步提交：kafkaConsumer.commitAsync()；异步提交】
        //同步提交：发起提交时应用会阻塞。可以减少手动提交的频率，但是会增加消息重复消费的概率
        //异步提交：如果服务器返回提交失败，异步提交不会进行重试。同步提交会进行重试直到成功或者最后抛出异常给应用。异步提交没有重试是因为如果同时存在多个异步提交，重试可能会导致位移覆盖。
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        //订阅topic
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                //尽量避免重复消费
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        });

        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info(record.offset() + ":" + record.value());
                    //异步提交消费位移，在发生再均衡动作之前可以通过再均衡监听器的onPartitionsRevoked回调执行commitSync方法同步提交位移
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }
    }
}
