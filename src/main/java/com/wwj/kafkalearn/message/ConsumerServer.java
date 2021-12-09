package com.wwj.kafkalearn.message;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Author: Mr.Thomas
 * @Date: 2020/10/22 002214:26
 */
@Slf4j
public class ConsumerServer {

    private static Map<String, KafkaConsumer<String, String>> consumerMap = new HashMap<String, KafkaConsumer<String, String>>();

    private static ConsumerServer consumerServer;

    public ConsumerServer() {
    }

    public static ConsumerServer getInstance() {
        if (null == consumerServer) {
            consumerServer = new ConsumerServer();
        }
        return consumerServer;
    }

    public KafkaConsumer<String, String> getKafkaConsumer(String topic, String groupId) {
        if (null == groupId) {
            if (consumerMap.containsKey(topic)) {
                return consumerMap.get(topic);
            } else {
                KafkaConsumer<String, String> consumer = createKafkaConsumer(topic, null);
                consumerMap.put(topic, consumer);
                return consumer;
            }
        } else {
            if (consumerMap.containsKey(topic + "_" + groupId)) {
                return consumerMap.get(topic);
            } else {
                KafkaConsumer<String, String> consumer = createKafkaConsumer(topic, groupId);
                consumerMap.put(topic + "_" + groupId, consumer);
                return consumer;
            }
        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String topic, String groupId) {
        groupId = groupId == null ? "test" : groupId;
        Map<String, Object> configs = new HashMap<String, Object>();
        //设置key的序列化器 反序列化 和生产者相对应
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //设置值序列化器
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //设置集群地址
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092,127.0.0.1:9093");
        //消费组，默认为空，如果设置为空则会抛出异常【写入一次消息，可支持任意多应用读取这个消息，使得每个应用都能读取到全量消息，应用需要有不同的消费组】
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //默认true，自动位移提交:消费者会在poll方法调用后每隔5秒【auto.commit.interval.ms指定】提交一次位移。可能会重复消费
        //false，手动提交位移【同步提交：kafkaConsumer.commitAsync()；异步提交】
        //同步提交：发起提交时应用会阻塞。可以减少手动提交的频率，但是会增加消息重复消费的概率
        //异步提交：如果服务器返回提交失败，异步提交不会进行重试。同步提交会进行重试直到成功或者最后抛出异常给应用。异步提交没有重试是因为如果同时存在多个异步提交，重试可能会导致位移覆盖。
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        //订阅topic
        consumer.subscribe(Arrays.asList(topic));
        //正则来匹配topic,订阅以test开头的topic
        //consumer.subscribe(Pattern.compile("test*"));
        //指定订阅的分区
        //consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
        return consumer;
    }

    /**
     * 位移提交
     * kafka分区，每条消息都有唯一的offset，用来表示消息在分区中的位置
     * 当我们调用poll()时，该方法会返回我们没有消费的消息。当消息从broker返回消费者时，broker并不跟踪这些消息是否被消费者接收到；kafka让消费者自身来管理消费的位移，并向消费者提供更新位移的接口，这种更新位移的方式成为提交(commit)。
     */
    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = ConsumerServer.getInstance().getKafkaConsumer("topic", null);
        while (true) {
            //1秒监听一次
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                continue;
            }
            records.forEach(o -> {
                String msg = o.value();
                log.info("msg:{}", msg);
            });
            //同步提交位移
            kafkaConsumer.commitAsync();
            //异步提交
            /*kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {

                }
            });*/
        }
    }
}
